# connectivity-edge-agent

Simulates a fleet of field devices streaming hardware telemetry to the cloud. Each device is a goroutine with its own sensors, local buffer and credentials, so one process can stand in for thousands of devices.

Part of [Connectivity](https://github.com/spr732/connectivity-platform), a mock edge-to-cloud telemetry system built for practicing production debugging.

## What each device does

1. Starts at a random offset inside the send interval, so the fleet doesn't tick in lockstep.
2. Registers with the enrollment key and receives a device token, retrying with jittered exponential backoff.
3. Every tick, reads its sensors (CPU temperature, fan speed, PSU voltage, memory, reallocated disk sectors) into a bounded buffer. Occasionally a device enters an overheat episode.
4. Sends buffered samples as a batch. The batch keeps its ID across retries so the backend can deduplicate it.
5. Sends a heartbeat every few ticks, and re-registers if its credentials are rejected.
6. Keeps a rolling system log of what happens to it: thermal warnings, failing disk sectors, PSU drift, network errors.
7. Collects a log bundle on a schedule, and immediately when CPU temperature crosses the fault threshold.

## Log bundles

A bundle is a `tar.gz` holding `inventory.json`, `logs/system.log` (the device's rolling log) and `logs/trace.log` (verbose diagnostics, sized by `BUNDLE_TRACE_BYTES`).

Bundles from every device go to one on-disk spool and are uploaded by a shared worker pool, the way a gateway caps concurrent transfers so log uploads can't starve telemetry on a thin WAN link.

- **Resumable.** Each attempt starts by asking the cloud which chunks it already has, then sends only the missing ones. A dropped connection or an agent restart costs one chunk, not the whole bundle.
- **Durable.** Spooled bundles survive restarts. The JSON metadata file is written last and acts as the commit marker; half-written bundles are cleaned up on start.
- **Prioritized.** Alert bundles upload before scheduled ones. When the spool is full, the oldest scheduled bundle is evicted first.
- **Compatible with an older cloud.** If the ingest API doesn't support bundles yet (404 on init), bundles stay spooled and are retried every few minutes.

| Upload outcome | What the agent does |
|---|---|
| 5xx, timeout, network error | Keeps the bundle and retries with backoff |
| 404 / 409 on a chunk or complete | Retries; the next init recreates the upload session |
| 400, 403, 413, 422 | Drops the bundle, because it can never upload, and counts it |
| Spool full | Evicts the oldest scheduled bundle and counts it |

## Failure behavior

| Situation | What the agent does |
|---|---|
| 5xx, timeout, network error, 429 | Keeps the batch and retries it next tick |
| Other 4xx | Drops the batch, because retrying can never succeed, and counts it |
| 401 | Re-registers, then retries the batch |
| Buffer full | Overwrites the oldest samples and counts the drops |
| Backend recovers | Drains the backlog, up to `MAX_DRAIN_BATCHES` per tick |

## Configuration

| Variable | Default |
|---|---|
| `INGEST_URL` | `http://localhost:8080` |
| `ENROLLMENT_KEY` | required |
| `DEVICE_COUNT` | `50` |
| `SITE_ID` / `DEVICE_PREFIX` | `site-a` / site ID |
| `SEND_INTERVAL` | `5s` |
| `HEARTBEAT_EVERY` | `6` ticks |
| `BUFFER_CAPACITY` | `2000` samples per device |
| `MAX_BATCH` / `MAX_DRAIN_BATCHES` | `200` / `5` |
| `HTTP_TIMEOUT` | `10s` |
| `METRICS_ADDR` | `:9102` |
| `LOG_LINES` | `1000` lines of system log per device |
| `BUNDLE_INTERVAL` | `30m` (`0` disables scheduled bundles) |
| `ALERT_BUNDLE_COOLDOWN` | `10m` between alert bundles per device |
| `BUNDLE_TRACE_BYTES` | `2097152` (about 1 MB per bundle after compression) |
| `SPOOL_DIR` | `/tmp/connectivity-spool` |
| `SPOOL_MAX_BUNDLES` | `200` |
| `UPLOAD_CONCURRENCY` | `4` |

Metrics are exposed at `/metrics` with the `connectivity_agent_` prefix.
