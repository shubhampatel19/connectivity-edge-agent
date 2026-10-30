# connectivity-edge-agent

Simulates a fleet of field devices streaming hardware telemetry to the cloud. Each device is a goroutine with its own sensors, local buffer and credentials, so one process can stand in for thousands of devices.

Part of [Connectivity](https://github.com/shubhampatel19/connectivity-platform), a mock edge-to-cloud telemetry system built for practicing production debugging.

## What each device does

1. Starts at a random offset inside the send interval, so the fleet doesn't tick in lockstep.
2. Registers with the enrollment key and receives a device token, retrying with jittered exponential backoff.
3. Every tick, reads its sensors (CPU temperature, fan speed, PSU voltage, memory, reallocated disk sectors) into a bounded buffer. Now and then a device overheats, its PSU rail sags, or its disk reallocates sectors.
4. Raises an alert when a fault starts, and sends queued alerts before telemetry.
5. Sends buffered samples as a batch. The batch keeps its ID across retries so the backend can deduplicate it.
6. Sends a heartbeat every few ticks, and re-registers if its credentials are rejected.
7. Keeps a rolling system log of what happens to it: thermal warnings, failing disk sectors, PSU drift, network errors.
8. Collects a log bundle on a schedule, and immediately when a thermal or PSU fault starts.

## Faults and alerts

| Fault | Starts when | Clears when | Alert | Log bundle |
|---|---|---|---|---|
| Overheat | CPU ≥ 90°C | CPU < 85°C | `thermal.critical`, critical | yes |
| PSU sag | 12V rail ≥ 0.20V off | rail < 0.15V off | `power.rail_out_of_range`, warning | yes |
| Disk wear | reallocated sector count rises | — | `storage.reallocated_sectors`, warning, value = count | no |

Each fault raises one alert when it starts. The gap between the start and clear thresholds (hysteresis) stops a noisy reading from raising a new alert every time it wobbles across the line. A sagging rail sits right at the threshold, so a sag still flaps and raises several alerts; the case service's rules are what tell a flapping rail from one stray reading.

How often faults happen on their own: an overheat or a PSU sag about once an hour across 300 devices at the default interval, and about 2% of devices have a degrading disk that keeps reallocating. `FAULT_RATE` scales all of it; `0` leaves only injected faults.

**Alerts carry their bundle.** When a fault wants logs, the device collects a bundle first and puts its ID on the alert, so the cloud can attach the logs to the case. Within `ALERT_BUNDLE_COOLDOWN` of the last alert bundle, the alert goes without one.

**The outbox.** Alerts wait in a per-device queue until the cloud (or the gateway) acknowledges them, and keep their `alert_id` across retries so the cloud can deduplicate.

| Alert send outcome | What the agent does |
|---|---|
| 5xx, timeout, network error, 429 | Keeps the alert and retries next tick |
| 404 / 405 | Keeps it: the cloud predates alerts. An agent upgraded before the cloud must not throw its faults away |
| Other 4xx | Drops it, because it can never succeed, and counts it |
| 401 | Re-registers, then retries |
| Outbox full (`ALERT_QUEUE_CAPACITY`) | Drops the oldest alert and counts it |

The outbox is in memory, so an agent restart loses alerts it hasn't sent. That is a deliberate gap: the bundle spool is on disk, the outbox isn't. Drill material.

## Injecting faults

For drills and CI, the metrics port also takes fault requests:

```bash
curl -X POST 'localhost:9102/debug/faults?kind=thermal'                  # one random device
curl -X POST 'localhost:9102/debug/faults?kind=thermal&devices=all'      # every device in this process: a site-wide cooling failure
curl -X POST 'localhost:9102/debug/faults?kind=psu&device=site-a-00007'  # one named device
```

`kind` is `thermal`, `psu` or `disk`. The response lists the devices hit. The fault starts on each device's next reading; an overheat takes about 15 readings to cross 90°C.

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

## Tracing

When `OTEL_EXPORTER_OTLP_ENDPOINT` is set, every telemetry send, alert and bundle upload is a span, and each request under it is a client span whose context goes to the cloud in a `traceparent` header. The cloud continues the same trace. A real device's traces would go out over the same WAN as its data; here they go straight to the cloud's collector, so you can still see failed sends during a WAN fault.

Set `OTEL_TRACES_SAMPLER=parentbased_traceidratio` and `OTEL_TRACES_SAMPLER_ARG=0.1` to trace one send in ten; the cloud follows each trace's decision.

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
| `ALERT_QUEUE_CAPACITY` | `100` alerts per device |
| `FAULT_RATE` | `1` (scales natural faults; `0` = injected only) |
| `BUNDLE_INTERVAL` | `30m` (`0` disables scheduled bundles) |
| `ALERT_BUNDLE_COOLDOWN` | `10m` between alert bundles per device |
| `BUNDLE_TRACE_BYTES` | `2097152` (about 1 MB per bundle after compression) |
| `SPOOL_DIR` | `/tmp/connectivity-spool` |
| `SPOOL_MAX_BUNDLES` | `200` |
| `UPLOAD_CONCURRENCY` | `4` |

Metrics are exposed at `/metrics` with the `connectivity_agent_` prefix. Alerts: `alerts_raised_total{code}`, `alerts_sent_total`, `alert_send_failures_total{reason}`, `alerts_rejected_total`, `alerts_dropped_total`, and the `alerts_pending` gauge.
