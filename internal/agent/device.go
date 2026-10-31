package agent

import (
	"context"
	crand "crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/shubhampatel19/connectivity-contracts/wire"
	"github.com/shubhampatel19/connectivity-go-kit/platform"
)

type DeviceConfig struct {
	SendInterval    time.Duration
	HeartbeatEvery  int // heartbeat every N send ticks
	MaxBatch        int
	MaxDrainBatches int // batches a device may send per tick while catching up
	AlertQueue      int // alerts a device holds while it can't reach the cloud
}

// BundleConfig controls log bundle collection. A zero Interval disables
// scheduled bundles; alert bundles are still collected for faults.
type BundleConfig struct {
	Interval      time.Duration
	AlertCooldown time.Duration
	TraceBytes    int
}

// Device is one simulated field device running in its own goroutine.
type Device struct {
	id, model, firmware, site string

	cfg       DeviceConfig
	bundleCfg BundleConfig
	client    *Client
	uploader  *Uploader // nil disables log bundles
	sensors   *Sensors
	buf       *Buffer
	alerts    *alertQueue
	logs      *LogRing
	m         *metrics
	log       *slog.Logger
	rng       *rand.Rand

	tokenMu sync.RWMutex
	token   string

	pending    *wire.TelemetryBatch // sent but not yet acknowledged
	hasPending atomic.Bool

	nextBundle      time.Time
	lastAlertBundle time.Time
	thermalFault    bool
	psuFault        bool
	lastReallocated float64

	faults *FaultLog // where started faults' true causes go; nil keeps none
}

// Token returns the device's current credential. The uploader reads it from
// other goroutines.
func (d *Device) Token() string {
	d.tokenMu.RLock()
	defer d.tokenMu.RUnlock()
	return d.token
}

func (d *Device) setToken(t string) {
	d.tokenMu.Lock()
	d.token = t
	d.tokenMu.Unlock()
}

// Run registers the device, then reads sensors and ships telemetry every tick
// until ctx is cancelled.
func (d *Device) Run(ctx context.Context) {
	// Start at a random offset inside the interval so the fleet doesn't tick
	// in lockstep and hit the backend in synchronized waves.
	if !sleep(ctx, time.Duration(d.rng.Int64N(int64(d.cfg.SendInterval)))) {
		return
	}
	if !d.register(ctx) {
		return
	}
	if d.bundleCfg.Interval > 0 {
		d.nextBundle = time.Now().Add(time.Duration(d.rng.Int64N(int64(d.bundleCfg.Interval))))
	}

	ticker := time.NewTicker(d.cfg.SendInterval)
	defer ticker.Stop()
	for tick := 1; ; tick++ {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			r := d.sensors.Read(now)
			if dropped := d.buf.Push(r.Samples...); dropped > 0 {
				d.m.samplesDropped.Add(float64(dropped))
			}
			d.raiseAlerts(now, d.observe(now, tick, r))
			d.sendAlerts(ctx)
			d.flush(ctx)
			if tick%d.cfg.HeartbeatEvery == 0 {
				d.heartbeat(ctx)
			}
			d.maybeCollect(now)
		}
	}
}

// observe writes what a real device's system log would say about this
// reading, and returns the faults that started on it. The firmware's own
// lines come first, then the agent's threshold checks, then the background
// chatter every device's log has, so evidence sits among ordinary lines the
// way it does on a real box.
func (d *Device) observe(now time.Time, tick int, r Reading) []alertSignal {
	for _, e := range r.Events {
		d.logs.Add(now, e.Level, e.Component, e.Msg)
	}
	v := make(map[string]float64, len(r.Samples))
	for _, s := range r.Samples {
		v[s.Metric] = s.Value
	}
	var started []alertSignal

	temp, fan := v["cpu_temp_c"], v["fan_rpm"]
	switch {
	case temp >= thermalFaultC:
		d.logs.Add(now, "ERROR", "thermal", fmt.Sprintf("cpu_temp=%.1fC over fault threshold %.0fC, fan=%.0frpm, throttling CPU", temp, thermalFaultC, fan))
	case temp >= thermalWarnC:
		d.logs.Add(now, "WARN", "thermal", fmt.Sprintf("cpu_temp=%.1fC over %.0fC, fan=%.0frpm", temp, thermalWarnC, fan))
	}
	switch {
	case !d.thermalFault && temp >= thermalFaultC:
		d.thermalFault = true
		started = append(started, alertSignal{
			code: wire.AlertThermalCritical, severity: wire.SeverityCritical, value: temp, wantsBundle: true, state: r.Thermal,
			message: fmt.Sprintf("cpu_temp=%.1fC over fault threshold %.0fC, fan=%.0frpm", temp, thermalFaultC, fan),
		})
	case d.thermalFault && temp < thermalClearC:
		d.thermalFault = false
		d.logs.Add(now, "INFO", "thermal", fmt.Sprintf("cpu_temp=%.1fC back under %.0fC, thermal fault cleared", temp, thermalClearC))
	}

	if re := v["disk_reallocated_sectors"]; re > d.lastReallocated {
		d.logs.Add(now, "WARN", "storage", fmt.Sprintf("disk0 reallocated sector count rose to %.0f", re))
		started = append(started, alertSignal{
			code: wire.AlertDiskReallocated, severity: wire.SeverityWarning, value: re, state: r.Disk,
			// The logs are what tell a worn disk (a sector or two, nothing
			// pending) from a failing one (I/O errors, pending sectors), so a
			// disk alert brings them like the others; the cooldown still limits
			// how often.
			wantsBundle: true,
			message:     fmt.Sprintf("disk0 reallocated sector count rose to %.0f", re),
		})
		d.lastReallocated = re
	}

	psu := v["psu_voltage_v"]
	dev := math.Abs(psu - 12)
	if dev > psuWarnV {
		d.logs.Add(now, "WARN", "power", fmt.Sprintf("PSU1 12V rail at %.3fV, outside +/-%.2fV", psu, psuWarnV))
	}
	switch {
	case !d.psuFault && dev >= psuAlertV:
		d.psuFault = true
		started = append(started, alertSignal{
			code: wire.AlertPSURail, severity: wire.SeverityWarning, value: psu, wantsBundle: true, state: r.PSU,
			message: fmt.Sprintf("PSU1 12V rail at %.3fV, outside +/-%.2fV", psu, psuAlertV),
		})
	case d.psuFault && dev < psuClearV:
		d.psuFault = false
	}

	if tick%12 == 0 {
		d.logs.Add(now, "INFO", "telemetry", fmt.Sprintf("sampled %d metrics, %d samples buffered", len(r.Samples), d.buf.Len()))
	}
	d.chatter(now)
	return started
}

// chatter writes the routine lines every device's log carries: time sync,
// RAID housekeeping, and the odd warning that has nothing to do with today's
// fault. A cause has to be found among these.
func (d *Device) chatter(now time.Time) {
	if d.rng == nil {
		return
	}
	p := d.rng.Float64()
	switch {
	case p < 1.0/240:
		d.logs.Add(now, "INFO", "ntp", fmt.Sprintf("clock synchronized to 10.%d.0.1, offset %+.1fms", 1+d.rng.IntN(3), d.rng.NormFloat64()*3))
	case p < 1.0/240+1.0/900:
		d.logs.Add(now, "INFO", "raid", "patrol read completed on VD0, 0 errors")
	case p < 1.0/240+1.0/900+1.0/1500:
		d.logs.Add(now, "WARN", "bmc", fmt.Sprintf("system event log %d%% full, oldest entries will be overwritten", 80+d.rng.IntN(15)))
	case p < 1.0/240+1.0/900+1.0/1500+1.0/2000:
		d.logs.Add(now, "WARN", "security", fmt.Sprintf("device certificate expires in %d days", 10+d.rng.IntN(20)))
	case p < 1.0/240+1.0/900+1.0/1500+1.0/2000+1.0/1200:
		d.logs.Add(now, "WARN", "kernel", "eth1: link down (port not cabled)")
	}
}

// maybeCollect collects a scheduled bundle when one is due.
func (d *Device) maybeCollect(now time.Time) {
	if d.uploader == nil || d.bundleCfg.Interval <= 0 || now.Before(d.nextBundle) {
		return
	}
	d.nextBundle = now.Add(d.bundleCfg.Interval)
	d.collect(now, wire.ReasonScheduled)
}

// packBundle builds a bundle from the current logs, ready to upload.
func (d *Device) packBundle(now time.Time, reason wire.BundleReason, id string) (wire.BundleInitRequest, []byte, error) {
	data, err := buildBundle(bundleInput{
		DeviceID: d.id, Model: d.model, Firmware: d.firmware, Site: d.site,
		Reason: reason, CollectedAt: now, Logs: d.logs.Snapshot(),
		TraceBytes: d.bundleCfg.TraceBytes, Rng: d.rng,
	})
	if err != nil {
		return wire.BundleInitRequest{}, nil, err
	}
	sum := sha256.Sum256(data)
	return wire.BundleInitRequest{
		BundleID: id, DeviceID: d.id, SizeBytes: int64(len(data)),
		SHA256: hex.EncodeToString(sum[:]), Reason: reason, CollectedAt: now.UTC(),
	}, data, nil
}

// collect builds a bundle from the current logs and hands it to the uploader.
// It returns the bundle ID, or "" if the bundle couldn't be built or spooled.
func (d *Device) collect(now time.Time, reason wire.BundleReason) string {
	req, data, err := d.packBundle(now, reason, newBatchID())
	if err != nil {
		d.log.Error("building log bundle failed", "reason", reason, "err", err)
		return ""
	}
	if err := d.uploader.Submit(req, data); err != nil {
		d.log.Error("spooling log bundle failed", "bundle_id", req.BundleID, "err", err)
		return ""
	}
	d.logs.Add(now, "INFO", "support", fmt.Sprintf("collected %s log bundle %s (%d bytes)", reason, req.BundleID, len(data)))
	d.log.Info("log bundle collected", "bundle_id", req.BundleID, "reason", reason, "bytes", len(data))
	return req.BundleID
}

// flush sends the pending batch, or cuts a new one from the buffer. A batch
// keeps its ID across retries so the backend can deduplicate it if an earlier
// attempt actually landed (e.g. the response timed out after Kafka acked).
func (d *Device) flush(ctx context.Context) {
	for i := 0; i < d.cfg.MaxDrainBatches; i++ {
		if d.pending == nil {
			samples := d.buf.Take(d.cfg.MaxBatch)
			if len(samples) == 0 {
				return
			}
			d.pending = &wire.TelemetryBatch{BatchID: newBatchID(), DeviceID: d.id, Samples: samples}
			d.hasPending.Store(true)
		}

		start := time.Now()
		sctx, span := platform.Tracer().Start(ctx, "send telemetry", trace.WithAttributes(
			attribute.String("device.id", d.id),
			attribute.String("site.id", d.site),
			attribute.String("batch.id", d.pending.BatchID),
			attribute.Int("batch.samples", len(d.pending.Samples)),
			attribute.Int("buffer.samples", d.buf.Len()),
		))
		err := d.client.SendTelemetry(sctx, d.id, d.Token(), *d.pending)
		platform.RecordError(sctx, err)
		span.End()
		d.m.sendDuration.Observe(time.Since(start).Seconds())

		switch {
		case err == nil:
			d.m.batchesSent.Inc()
			d.m.samplesSent.Add(float64(len(d.pending.Samples)))
			d.clearPending()
			continue // backlog: keep draining up to MaxDrainBatches
		case ctx.Err() != nil:
			return
		case errors.Is(err, ErrUnauthorized):
			d.m.sendFailures.WithLabelValues("unauthorized").Inc()
			d.log.Warn("credentials rejected, re-registering")
			d.register(ctx) // the pending batch is retried on the next tick
		case isPermanent(err):
			d.m.batchesRejected.Inc()
			d.log.ErrorContext(sctx, "backend rejected batch permanently, dropping it",
				"batch_id", d.pending.BatchID, "samples", len(d.pending.Samples), "err", err)
			d.logs.Add(time.Now(), "ERROR", "connectivity", fmt.Sprintf("telemetry batch rejected by cloud: %v", err))
			d.clearPending()
			continue
		default:
			reason := errorReason(err)
			d.m.sendFailures.WithLabelValues(reason).Inc()
			d.log.WarnContext(sctx, "send failed, will retry next tick",
				"batch_id", d.pending.BatchID, "reason", reason, "err", err)
			d.logs.Add(time.Now(), "WARN", "connectivity", fmt.Sprintf("telemetry upload failed (%s), %d samples buffered", reason, d.buf.Len()))
		}
		return
	}
}

func (d *Device) clearPending() {
	d.pending = nil
	d.hasPending.Store(false)
}

func (d *Device) heartbeat(ctx context.Context) {
	err := d.client.Heartbeat(ctx, d.id, d.Token())
	switch {
	case err == nil, ctx.Err() != nil:
	case errors.Is(err, ErrUnauthorized):
		d.m.heartbeatFailures.WithLabelValues("unauthorized").Inc()
		d.register(ctx)
	default:
		d.m.heartbeatFailures.WithLabelValues(errorReason(err)).Inc()
		d.log.Debug("heartbeat failed", "err", err)
	}
}

// register retries until it succeeds or ctx is cancelled, using exponential
// backoff with jitter so a fleet recovering from an outage doesn't stampede.
func (d *Device) register(ctx context.Context) bool {
	req := wire.RegisterRequest{DeviceID: d.id, Model: d.model, Firmware: d.firmware, SiteID: d.site}
	for attempt := 1; ; attempt++ {
		token, err := d.client.Register(ctx, req)
		if err == nil {
			d.setToken(token)
			d.m.registrations.Inc()
			d.log.Info("registered", "attempt", attempt)
			d.logs.Add(time.Now(), "INFO", "connectivity", "registered with cloud")
			return true
		}
		if ctx.Err() != nil {
			return false
		}
		d.m.registerFailures.WithLabelValues(errorReason(err)).Inc()
		wait := backoff(attempt, time.Second, 30*time.Second, d.rng)
		d.log.Warn("registration failed", "attempt", attempt, "retry_in", wait.String(), "err", err)
		if !sleep(ctx, wait) {
			return false
		}
	}
}

// backoff returns base*2^(attempt-1), capped at ceiling, with "equal jitter":
// a random value between half and all of that delay.
func backoff(attempt int, base, ceiling time.Duration, rng *rand.Rand) time.Duration {
	d := ceiling
	if shift := attempt - 1; shift < 30 {
		if v := base << shift; v > 0 && v < ceiling {
			d = v
		}
	}
	half := d / 2
	return half + time.Duration(rng.Int64N(int64(half)+1))
}

func sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// newBatchID uses crypto/rand so IDs stay unique across agent restarts. A
// repeated ID would make the backend silently discard real data as a duplicate.
func newBatchID() string {
	b := make([]byte, 16)
	_, _ = crand.Read(b)
	return hex.EncodeToString(b)
}

func newRand() *rand.Rand {
	var seed [16]byte
	_, _ = crand.Read(seed[:])
	return rand.New(rand.NewPCG(binary.LittleEndian.Uint64(seed[:8]), binary.LittleEndian.Uint64(seed[8:])))
}
