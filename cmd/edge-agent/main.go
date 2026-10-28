// Command edge-agent simulates a fleet of field devices streaming telemetry.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spr732/connectivity-contracts/wire"
	"github.com/spr732/connectivity-edge-agent/internal/agent"
	"github.com/spr732/connectivity-go-kit/platform"
)

func main() {
	log := platform.NewLogger("edge-agent")
	if err := run(log); err != nil {
		log.Error("exiting", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	var env platform.Env
	ingestURL := env.String("INGEST_URL", "http://localhost:8080")
	enrollmentKey := env.Required("ENROLLMENT_KEY")
	siteID := env.String("SITE_ID", "site-a")
	prefix := env.String("DEVICE_PREFIX", siteID)
	count := env.Int("DEVICE_COUNT", 50)
	firmware := env.String("FIRMWARE_VERSION", "1.4.2")
	interval := env.Duration("SEND_INTERVAL", 5*time.Second)
	heartbeatEvery := env.Int("HEARTBEAT_EVERY", 6)
	bufferCap := env.Int("BUFFER_CAPACITY", 2000)
	maxBatch := env.Int("MAX_BATCH", 200)
	maxDrain := env.Int("MAX_DRAIN_BATCHES", 5)
	httpTimeout := env.Duration("HTTP_TIMEOUT", 10*time.Second)
	metricsAddr := env.String("METRICS_ADDR", ":9102")

	env.Check(count > 0, "DEVICE_COUNT must be positive")
	env.Check(interval > 0, "SEND_INTERVAL must be positive")
	env.Check(heartbeatEvery > 0, "HEARTBEAT_EVERY must be positive")
	env.Check(bufferCap > 0, "BUFFER_CAPACITY must be positive")
	env.Check(maxDrain > 0, "MAX_DRAIN_BATCHES must be positive")
	env.Check(maxBatch > 0 && maxBatch <= wire.MaxSamplesPerBatch,
		fmt.Sprintf("MAX_BATCH must be between 1 and %d", wire.MaxSamplesPerBatch))
	if err := env.Err(); err != nil {
		return err
	}

	reg := platform.NewRegistry()
	client := agent.NewClient(ingestURL, enrollmentKey, httpTimeout)
	fleet := agent.NewFleet(agent.FleetConfig{
		SiteID:         siteID,
		DevicePrefix:   prefix,
		Count:          count,
		Firmware:       firmware,
		BufferCapacity: bufferCap,
		Device: agent.DeviceConfig{
			SendInterval:    interval,
			HeartbeatEvery:  heartbeatEvery,
			MaxBatch:        maxBatch,
			MaxDrainBatches: maxDrain,
		},
	}, client, log, reg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		if err := platform.Serve(ctx, log, metricsAddr, platform.MetricsHandler(reg)); err != nil {
			log.Error("metrics server failed", "err", err)
			stop()
		}
	}()

	log.Info("starting fleet", "devices", count, "site", siteID, "ingest_url", ingestURL, "interval", interval.String())
	fleet.Run(ctx)
	log.Info("fleet stopped")
	return nil
}
