# connectivity-edge-agent: design

One process simulates a site's whole fleet. Each simulated device is a goroutine with its own sensors, buffer, alert outbox, system log and clock, and behaves the way the real embedded agent does: it never blocks on the cloud, it bounds everything it keeps, and it assumes the network is unreliable.

## Layout

| File | Responsibility |
|---|---|
| `cmd/edge-agent/main.go` | Configuration and wiring |
| `cmd/bundlegen/main.go` | Writes labelled log bundles for AI triage evals, offline |
| `internal/agent/fleet.go` | Builds the devices, serves `/debug/faults` and `/debug/logs`, spreads firmware versions and clock skew |
| `internal/agent/device.go` | One device's loop: read, buffer, alert, send, heartbeat, collect |
| `internal/agent/sensors.go`, `causes.go` | The physics: readings, faults, and the root cause behind each fault |
| `internal/agent/alerts.go` | Thresholds with hysteresis, the alert outbox, sending alerts |
| `internal/agent/buffer.go` | The bounded ring of unsent samples |
| `internal/agent/logs.go` | The device's rolling system log |
| `internal/agent/bundle.go`, `spool.go`, `uploader.go` | Packing a log bundle, keeping it on disk, uploading it resumably |
| `internal/agent/client.go` | HTTP to the cloud or the gateway |
| `internal/agent/faultlog.go` | The answer key: the true cause of each alert |
| `internal/agent/simulate.go` | The same device, run offline on a fake clock |

## One tick

Every `SEND_INTERVAL` (5 seconds), in this order:

```
read sensors ─▶ push samples to buffer ─▶ raise alerts ─▶ send alerts ─▶ send telemetry ─▶ heartbeat (every 6th tick) ─▶ maybe collect a bundle
```

Alerts go before telemetry because an alert is what opens a support case; if the link allows only one request this tick, it should be that one.

Devices start at a random offset inside the interval, so 200 devices don't all send in the same millisecond.

## Telemetry: buffer, batch, retry

Samples wait in `Buffer`, a ring of `BUFFER_CAPACITY` samples. When it is full the oldest are overwritten and counted. On a device with fixed memory there is no alternative, and recent readings are worth more than old ones.

`flush` takes up to `MAX_BATCH` samples as a pending batch and gives it a `batch_id` once. That same batch, with that same ID, is retried until the cloud takes it or refuses it for good:

| Response | What the device does |
|---|---|
| 2xx | Clears the batch; if more is buffered, sends up to `MAX_DRAIN_BATCHES` in this tick |
| 5xx, 429, 408, timeout, network error | Keeps the batch for the next tick |
| 401 | Registers again, then retries |
| Other 4xx | Drops the batch and counts it: retrying can't succeed, and keeping it would block everything behind it |

Reusing the ID is what makes retries safe. The device can't know whether a timed-out request was stored; the cloud deduplicates on the ID.

## Alerts

Each fault has two thresholds: it starts above one and clears below a lower one (`alerts.go`). Without that gap a reading hovering at the threshold would raise an alert every few seconds. An alert is raised once, when the fault starts.

Alerts wait in a per-device outbox of `ALERT_QUEUE_CAPACITY`. The outbox is in memory, so an agent restart loses alerts that were not yet delivered. That is a known gap, left in as drill material: compare it with site B, where the gateway takes the alert onto disk.

## Log bundles

A bundle is a gzipped tar of the device's recent system log plus a trace blob, collected on a schedule and when an alert is raised (with a cooldown, so a flapping fault doesn't produce a bundle a minute).

```
collect ─▶ Spool (disk) ─▶ Uploader workers ─▶ init ─▶ missing chunks ─▶ complete ─▶ delete from spool
```

- **The spool is the durability boundary.** Each bundle is `<id>.tgz` and `<id>.json`. The JSON is written last and is the commit marker: a crash mid-write leaves a `.tgz` with no JSON, which is deleted at startup.
- **The spool is bounded.** When full, scheduled bundles are evicted before alert bundles.
- **Uploads resume.** Init returns which chunks the cloud already has; only the rest are sent. A restart mid-upload costs at most one chunk.
- **Uploads can't starve telemetry of workers.** They run on their own pool of `UPLOAD_CONCURRENCY` goroutines. They do share the site's link.
- **Bundle transfers have their own timeout.** A chunk or a completion gets `BUNDLE_HTTP_TIMEOUT` (2 minutes), not `HTTP_TIMEOUT` (10 seconds). A chunk that has to fit in 10 seconds needs a fast link; on a slow one it would fail forever, and every failed attempt would waste the bandwidth it used.

## Root causes and the answer key

A fault has a cause the cloud never sees. An overheat is a dead fan, blocked airflow or a hot room; each leaves different evidence in the readings and the system log (`causes.go`, `sensors.go`). The agent records the true cause of every alert in `FaultLog`, served at `GET /debug/faults`, so a diagnosis, human or AI, can be graded. `POST /debug/faults` starts a fault on demand, and `POST /debug/logs` plants log lines.

`simulate.go` runs the same device code on a fake clock with no network. It is deterministic, which is what lets `bundlegen` produce the same labelled eval cases every time.

## A fleet that isn't uniform

Real fleets are never identical, and incidents often hide in the difference.

- **`FIRMWARE_MIX`** spreads devices over versions (`1.4.2=70,1.5.0=30`). Device N always gets the same version. Useful for incidents that affect only one version.
- **`CLOCK_SKEW_MAX`** gives each device a fixed clock error. Every timestamp the device produces (samples, alerts, log lines, bundle collection times) uses `Device.clock`. The cloud accepts late timestamps and rejects ones more than 10 minutes ahead, so a large skew turns into 400s and dropped batches for some devices only.

## Why one process

It is cheap to run 300 devices this way, and one `Client` with a shared connection pool mirrors how a gateway multiplexes devices. The cost to realism: killing the process kills every device at a site at once, and all of them share one network path. Per-device network faults have to be simulated in the agent rather than on the wire.

## Known limits

- The alert outbox and the telemetry buffer are in memory.
- The device token is in memory; after a restart every device registers again.
- Traces go straight to the collector, not over the WAN, so tracing keeps working during a WAN fault.
