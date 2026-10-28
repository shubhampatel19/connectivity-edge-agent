# connectivity-edge-agent

Simulates a fleet of field devices streaming hardware telemetry to the cloud. Each device is a goroutine with its own sensors, local buffer and credentials, so one process can stand in for thousands of devices.

Part of [Connectivity](https://github.com/spr732/connectivity-platform), a mock edge-to-cloud telemetry system built for practicing production debugging.

## What each device does

1. Starts at a random offset inside the send interval, so the fleet doesn't tick in lockstep.
2. Registers with the enrollment key and receives a device token, retrying with jittered exponential backoff.
3. Every tick, reads its sensors (CPU temperature, fan speed, PSU voltage, memory, reallocated disk sectors) into a bounded buffer. Occasionally a device enters an overheat episode.
4. Sends buffered samples as a batch. The batch keeps its ID across retries so the backend can deduplicate it.
5. Sends a heartbeat every few ticks, and re-registers if its credentials are rejected.

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

Metrics are exposed at `/metrics` with the `connectivity_agent_` prefix.
