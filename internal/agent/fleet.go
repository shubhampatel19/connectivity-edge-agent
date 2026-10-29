// Package agent simulates a fleet of field devices. Each device is a goroutine
// with its own sensors, buffer, system log and credentials, so one process can
// stand in for thousands of devices. Log bundles from every device share one
// on-disk spool and one upload worker pool, as they would behind a gateway.
package agent

import (
	"context"
	"fmt"
	"log/slog"
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

	// Bundle uploads. An empty SpoolDir disables log bundles entirely.
	SpoolDir string
	SpoolMax int
	Uploads  UploaderConfig
}

var models = []string{"edge-server-e1", "storage-node-s2", "network-switch-n3"}

type Fleet struct {
	devices  []*Device
	uploader *Uploader
}

func NewFleet(cfg FleetConfig, client *Client, log *slog.Logger, reg prometheus.Registerer) (*Fleet, error) {
	m := newMetrics(reg)
	f := &Fleet{devices: make([]*Device, cfg.Count)}
	byID := make(map[string]*Device, cfg.Count)

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
			sensors: NewSensors(newRand()),
			buf:     NewBuffer(cfg.BufferCapacity),
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
