package agent

import (
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/spr732/connectivity-contracts/wire"
)

type DeviceConfig struct {
	SendInterval    time.Duration
	HeartbeatEvery  int // heartbeat every N send ticks
	MaxBatch        int
	MaxDrainBatches int // batches a device may send per tick while catching up
}

// Device is one simulated field device running in its own goroutine.
type Device struct {
	id, model, firmware, site string

	cfg     DeviceConfig
	client  *Client
	sensors *Sensors
	buf     *Buffer
	m       *metrics
	log     *slog.Logger
	rng     *rand.Rand

	token      string
	pending    *wire.TelemetryBatch // sent but not yet acknowledged
	hasPending atomic.Bool
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

	ticker := time.NewTicker(d.cfg.SendInterval)
	defer ticker.Stop()
	for tick := 1; ; tick++ {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if dropped := d.buf.Push(d.sensors.Read(now)...); dropped > 0 {
				d.m.samplesDropped.Add(float64(dropped))
			}
			d.flush(ctx)
			if tick%d.cfg.HeartbeatEvery == 0 {
				d.heartbeat(ctx)
			}
		}
	}
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
		err := d.client.SendTelemetry(ctx, d.id, d.token, *d.pending)
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
			d.log.Error("backend rejected batch permanently, dropping it",
				"batch_id", d.pending.BatchID, "samples", len(d.pending.Samples), "err", err)
			d.clearPending()
			continue
		default:
			reason := errorReason(err)
			d.m.sendFailures.WithLabelValues(reason).Inc()
			d.log.Warn("send failed, will retry next tick",
				"batch_id", d.pending.BatchID, "reason", reason, "err", err)
		}
		return
	}
}

func (d *Device) clearPending() {
	d.pending = nil
	d.hasPending.Store(false)
}

func (d *Device) heartbeat(ctx context.Context) {
	err := d.client.Heartbeat(ctx, d.id, d.token)
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
			d.token = token
			d.m.registrations.Inc()
			d.log.Info("registered", "attempt", attempt)
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
