// Package agent simulates a fleet of field devices. Each device is a goroutine
// with its own sensors, buffer and credentials, so one process can stand in for
// thousands of devices.
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
	Device         DeviceConfig
}

var models = []string{"edge-server-e1", "storage-node-s2", "network-switch-n3"}

type Fleet struct {
	devices []*Device
}

func NewFleet(cfg FleetConfig, client *Client, log *slog.Logger, reg prometheus.Registerer) *Fleet {
	m := newMetrics(reg)
	f := &Fleet{devices: make([]*Device, cfg.Count)}
	for i := range f.devices {
		id := fmt.Sprintf("%s-%05d", cfg.DevicePrefix, i+1)
		f.devices[i] = &Device{
			id: id, model: models[i%len(models)], firmware: cfg.Firmware, site: cfg.SiteID,
			cfg: cfg.Device, client: client,
			sensors: NewSensors(newRand()),
			buf:     NewBuffer(cfg.BufferCapacity),
			m:       m,
			log:     log.With("device_id", id),
			rng:     newRand(),
		}
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
	return f
}

// Run starts every device and blocks until ctx is cancelled and all devices
// have stopped.
func (f *Fleet) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, d := range f.devices {
		wg.Add(1)
		go func(d *Device) {
			defer wg.Done()
			d.Run(ctx)
		}(d)
	}
	wg.Wait()
}
