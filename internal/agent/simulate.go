package agent

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/shubhampatel19/connectivity-contracts/wire"
)

// SimConfig describes one fault run offline: the same sensors, system log,
// alerts and bundle format as a live device, ticked on a fake clock with no
// cloud. A run is deterministic: the same config gives the same alerts and
// the same bundle, byte for byte. Evals use it to build labelled cases.
type SimConfig struct {
	Seed                              uint64
	DeviceID, SiteID, Model, Firmware string
	Fault                             Fault
	Cause                             Cause
	Start                             time.Time     // the first tick
	Interval                          time.Duration // between ticks; default 5s, as a live device
	Warmup                            int           // normal ticks before the fault, so the log has history; default 360
	After                             int           // ticks to keep going after the first alert; default 24
	LogLines                          int           // system log ring size; default 1000, as a live device
	TraceBytes                        int           // size of trace.log in the bundle; default 64 KiB
	TelemetryTicks                    int           // how much telemetry the result keeps, counted back from the end; default 120
	// Plant writes extra lines into the log at random ticks during the
	// warmup, for cases where something unusual is in the log, such as a
	// prompt injection.
	Plant []SensorEvent
}

// SimResult is what the cloud would have received from the run, plus the
// answer key.
type SimResult struct {
	Alerts    []wire.DeviceAlert // every alert from the fault onwards, in order
	BundleID  string             // the alert bundle, if any alert wanted one
	Bundle    []byte
	Telemetry []wire.Sample
	Record    FaultRecord // the answer key for the first alert
}

// Simulate runs one device through a fault.
func Simulate(cfg SimConfig) (SimResult, error) {
	if cfg.Cause == "" || cfg.Cause.Fault() != cfg.Fault {
		return SimResult{}, fmt.Errorf("cause %q is not a cause of a %s fault", cfg.Cause, cfg.Fault)
	}
	def := func(v *int, d int) {
		if *v <= 0 {
			*v = d
		}
	}
	def(&cfg.Warmup, 360)
	def(&cfg.After, 24)
	def(&cfg.LogLines, 1000)
	def(&cfg.TraceBytes, 64<<10)
	def(&cfg.TelemetryTicks, 120)
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Second
	}

	seeded := func(stream uint64) *rand.Rand { return rand.New(rand.NewPCG(cfg.Seed, stream)) }
	ids := seeded(3)
	newID := func() string {
		var b [16]byte
		for i := 0; i < 16; i += 8 {
			v := ids.Uint64()
			for k := 0; k < 8; k++ {
				b[i+k] = byte(v >> (8 * k))
			}
		}
		return hex.EncodeToString(b[:])
	}
	answers := NewFaultLog(64)
	d := &Device{
		id: cfg.DeviceID, model: cfg.Model, firmware: cfg.Firmware, site: cfg.SiteID,
		bundleCfg: BundleConfig{TraceBytes: cfg.TraceBytes},
		sensors:   NewSensors(seeded(1), 0), // no natural faults: only the one under test
		buf:       NewBuffer(1),
		logs:      NewLogRing(cfg.LogLines),
		log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		rng:       seeded(2),
		faults:    answers,
	}
	plantAt := make(map[int][]SensorEvent, len(cfg.Plant))
	for _, p := range cfg.Plant {
		t := cfg.Warmup/4 + d.rng.IntN(max(cfg.Warmup*3/4, 1))
		plantAt[t] = append(plantAt[t], p)
	}

	var res SimResult
	injected := false
	collect := func(now time.Time) string {
		if res.Bundle != nil || !injected {
			return "" // one bundle per run, as the live cooldown would allow, and none for noise before the fault
		}
		req, data, err := d.packBundle(now, wire.ReasonAlert, newID())
		if err != nil {
			return ""
		}
		res.Bundle, res.BundleID = data, req.BundleID
		d.logs.Add(now, "INFO", "support", fmt.Sprintf("collected %s log bundle %s (%d bytes)", wire.ReasonAlert, req.BundleID, len(data)))
		return req.BundleID
	}

	keep := cfg.TelemetryTicks
	var window [][]wire.Sample
	firstAlert := 0
	for tick := 1; ; tick++ {
		if firstAlert == 0 && tick > cfg.Warmup+600 {
			return SimResult{}, errors.New("the fault never raised an alert")
		}
		if firstAlert > 0 && tick > firstAlert+cfg.After {
			break
		}
		now := cfg.Start.Add(time.Duration(tick) * cfg.Interval)
		for _, p := range plantAt[tick] {
			d.logs.Add(now, p.Level, p.Component, p.Msg)
		}
		if tick == cfg.Warmup+1 {
			d.sensors.Inject(cfg.Fault, cfg.Cause)
			injected = true
		}
		r := d.sensors.Read(now)
		window = append(window, r.Samples)
		if len(window) > keep {
			window = window[1:]
		}
		alerts := d.makeAlerts(now, d.observe(now, tick, r), collect, newID)
		if tick <= cfg.Warmup {
			continue // noise before the fault isn't part of the case
		}
		if len(alerts) > 0 && firstAlert == 0 {
			firstAlert = tick
		}
		res.Alerts = append(res.Alerts, alerts...)
	}
	for _, w := range window {
		res.Telemetry = append(res.Telemetry, w...)
	}
	for _, rec := range answers.Recent(64, "") {
		if rec.AlertID == res.Alerts[0].AlertID {
			res.Record = rec
		}
	}
	return res, nil
}
