// Package agent simulates a fleet of field devices. Each device is a goroutine
// with its own sensors, buffer, system log and credentials, so one process can
// stand in for thousands of devices. Log bundles from every device share one
// on-disk spool and one upload worker pool, as they would behind a gateway.
package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type FleetConfig struct {
	SiteID         string
	DevicePrefix   string
	Count          int
	Firmware       string
	BufferCapacity int
	LogLines       int
	Device         DeviceConfig
	Bundles        BundleConfig
	// FaultRate scales how often faults start on their own: 1 is the
	// default mix, 0 leaves only faults injected through /debug/faults.
	FaultRate float64

	// Bundle uploads. An empty SpoolDir disables log bundles entirely.
	SpoolDir string
	SpoolMax int
	Uploads  UploaderConfig
}

var models = []string{"edge-server-e1", "storage-node-s2", "network-switch-n3"}

type Fleet struct {
	devices  []*Device
	byID     map[string]*Device
	uploader *Uploader
	log      *slog.Logger
}

func NewFleet(cfg FleetConfig, client *Client, log *slog.Logger, reg prometheus.Registerer) (*Fleet, error) {
	m := newMetrics(reg)
	byID := make(map[string]*Device, cfg.Count)
	f := &Fleet{devices: make([]*Device, cfg.Count), byID: byID, log: log}
	alertQueue := cfg.Device.AlertQueue
	if alertQueue <= 0 {
		alertQueue = 100
	}

	if cfg.SpoolDir != "" {
		spool, err := OpenSpool(cfg.SpoolDir, cfg.SpoolMax, log)
		if err != nil {
			return nil, err
		}
		tokenFor := func(id string) string {
			if d := byID[id]; d != nil {
				return d.Token()
			}
			return ""
		}
		f.uploader = NewUploader(cfg.Uploads, client, spool, tokenFor, m, log)
		reg.MustRegister(
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "connectivity_agent_spool_bundles",
				Help: "Log bundles waiting in the on-disk spool.",
			}, func() float64 { n, _ := spool.Stats(); return float64(n) }),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "connectivity_agent_spool_bytes",
				Help: "Bytes of log bundles waiting in the on-disk spool.",
			}, func() float64 { _, b := spool.Stats(); return float64(b) }),
		)
	}

	for i := range f.devices {
		id := fmt.Sprintf("%s-%05d", cfg.DevicePrefix, i+1)
		d := &Device{
			id: id, model: models[i%len(models)], firmware: cfg.Firmware, site: cfg.SiteID,
			cfg: cfg.Device, bundleCfg: cfg.Bundles, client: client, uploader: f.uploader,
			sensors: NewSensors(newRand(), cfg.FaultRate),
			buf:     NewBuffer(cfg.BufferCapacity),
			alerts:  newAlertQueue(alertQueue),
			logs:    NewLogRing(cfg.LogLines),
			m:       m,
			log:     log.With("device_id", id),
			rng:     newRand(),
		}
		f.devices[i] = d
		byID[id] = d
	}

	reg.MustRegister(
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "connectivity_agent_devices",
			Help: "Simulated devices in this agent process.",
		}, func() float64 { return float64(len(f.devices)) }),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "connectivity_agent_buffered_samples",
			Help: "Samples waiting in device buffers across the fleet.",
		}, func() float64 {
			total := 0
			for _, d := range f.devices {
				total += d.buf.Len()
			}
			return float64(total)
		}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "connectivity_agent_alerts_pending",
			Help: "Alerts waiting in device outboxes across the fleet.",
		}, func() float64 {
			total := 0
			for _, d := range f.devices {
				total += d.alerts.Len()
			}
			return float64(total)
		}),
		prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "connectivity_agent_devices_with_pending_batch",
			Help: "Devices holding an unacknowledged batch.",
		}, func() float64 {
			n := 0
			for _, d := range f.devices {
				if d.hasPending.Load() {
					n++
				}
			}
			return float64(n)
		}),
	)
	return f, nil
}

// InjectFault forces a fault on the next reading of the named device, or of
// n devices picked at random (n <= 0 or n beyond the fleet size means all).
// It returns the IDs of the devices it hit.
func (f *Fleet) InjectFault(fault Fault, deviceID string, n int) ([]string, error) {
	if deviceID != "" {
		d := f.byID[deviceID]
		if d == nil {
			return nil, fmt.Errorf("no device %q in this fleet", deviceID)
		}
		d.sensors.Inject(fault)
		return []string{deviceID}, nil
	}
	if n <= 0 || n > len(f.devices) {
		n = len(f.devices)
	}
	ids := make([]string, 0, n)
	for _, i := range rand.Perm(len(f.devices))[:n] {
		f.devices[i].sensors.Inject(fault)
		ids = append(ids, f.devices[i].id)
	}
	return ids, nil
}

// FaultHandler serves POST /debug/faults?kind=thermal|psu|disk with either
// device=<id> or devices=<n|all> (default 1). It exists for drills and CI: a
// fault on demand instead of waiting for one.
func (f *Fleet) FaultHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		fault, err := ParseFault(q.Get("kind"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		n := 1
		switch v := q.Get("devices"); v {
		case "":
		case "all":
			n = 0
		default:
			if n, err = strconv.Atoi(v); err != nil || n < 1 {
				http.Error(w, "devices must be a positive number or 'all'", http.StatusBadRequest)
				return
			}
		}
		ids, err := f.InjectFault(fault, q.Get("device"), n)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		f.log.Warn("fault injected", "kind", q.Get("kind"), "devices", len(ids))
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"kind": q.Get("kind"), "devices": ids})
	})
}

// Run starts every device and the uploader, and blocks until ctx is cancelled
// and all of them have stopped.
func (f *Fleet) Run(ctx context.Context) {
	var wg sync.WaitGroup
	if f.uploader != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f.uploader.Run(ctx)
		}()
	}
	for _, d := range f.devices {
		wg.Add(1)
		go func(d *Device) {
			defer wg.Done()
			d.Run(ctx)
		}(d)
	}
	wg.Wait()
}
