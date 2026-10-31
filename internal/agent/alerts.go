package agent

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/shubhampatel19/connectivity-contracts/wire"
	"github.com/shubhampatel19/connectivity-go-kit/platform"
)

// Fault thresholds. Each fault has hysteresis: it starts at one threshold and
// clears only below a lower one, the way device firmware avoids raising a new
// alert every time a noisy reading wobbles across the line.
const (
	thermalWarnC  = 80.0
	thermalFaultC = 90.0
	thermalClearC = 85.0

	psuWarnV  = 0.15 // deviation from 12V that gets a WARN in the system log
	psuAlertV = 0.20 // deviation that raises an alert
	psuClearV = 0.15

	maxAlertsPerTick = 5
)

// alertSignal is a fault that just started, before it becomes a wire alert.
type alertSignal struct {
	code        wire.AlertCode
	severity    wire.AlertSeverity
	value       float64
	message     string
	wantsBundle bool // collect a log bundle to go with it
	state       FaultState
}

// alertQueue is a device's outbox: alerts wait here until the cloud (or the
// gateway) acknowledges them. It is in memory, so an agent restart loses
// unsent alerts; bounded, so a long outage drops the oldest instead of
// growing without limit. Only the device's own goroutine touches items.
type alertQueue struct {
	items []wire.DeviceAlert
	max   int
	n     atomic.Int64 // len(items), readable from the metrics goroutine
}

func newAlertQueue(max int) *alertQueue { return &alertQueue{max: max} }

// push adds an alert and reports how many old ones were dropped to fit it.
func (q *alertQueue) push(a wire.DeviceAlert) (dropped int) {
	q.items = append(q.items, a)
	for len(q.items) > q.max {
		q.items = q.items[1:]
		dropped++
	}
	q.n.Store(int64(len(q.items)))
	return dropped
}

func (q *alertQueue) peek() (wire.DeviceAlert, bool) {
	if len(q.items) == 0 {
		return wire.DeviceAlert{}, false
	}
	return q.items[0], true
}

func (q *alertQueue) pop() {
	if len(q.items) > 0 {
		q.items = q.items[1:]
		q.n.Store(int64(len(q.items)))
	}
}

func (q *alertQueue) Len() int {
	if q == nil {
		return 0
	}
	return int(q.n.Load())
}

// raiseAlerts turns this tick's fault signals into queued alerts. If any of
// them wants logs, one alert bundle is collected first (subject to the
// cooldown) and its ID goes on those alerts, so the cloud can attach it.
func (d *Device) raiseAlerts(now time.Time, signals []alertSignal) {
	for _, a := range d.makeAlerts(now, signals, d.collectAlertBundle, newBatchID) {
		if dropped := d.alerts.push(a); dropped > 0 {
			d.m.alertsDropped.Add(float64(dropped))
			d.log.Warn("alert outbox full, dropped oldest alerts", "dropped", dropped)
		}
		d.m.alertsRaised.WithLabelValues(string(a.Code)).Inc()
		d.log.Info("alert raised", "alert_id", a.AlertID, "code", a.Code, "value", a.Value, "bundle_id", a.BundleID)
	}
}

// makeAlerts builds the alerts for this tick's signals, logs them in the
// system log, and records each one's true cause in the answer key. collect
// gathers a bundle and returns its ID; the live device spools it for upload,
// the offline simulator keeps it.
func (d *Device) makeAlerts(now time.Time, signals []alertSignal, collect func(time.Time) string, newID func() string) []wire.DeviceAlert {
	if len(signals) == 0 {
		return nil
	}
	var bundleID string
	for _, s := range signals {
		if s.wantsBundle {
			bundleID = collect(now)
			break
		}
	}
	out := make([]wire.DeviceAlert, 0, len(signals))
	for _, s := range signals {
		a := wire.DeviceAlert{
			AlertID: newID(), DeviceID: d.id, SiteID: d.site,
			Code: s.code, Severity: s.severity, Value: s.value, Message: s.message,
			ObservedAt: now.UTC(),
		}
		if s.wantsBundle {
			a.BundleID = bundleID
		}
		d.logs.Add(now, "INFO", "support", fmt.Sprintf("raised alert %s %s (value %.3f)", a.Code, a.AlertID, a.Value))
		if s.state.Cause != "" {
			d.faults.Add(FaultRecord{
				DeviceID: d.id, SiteID: d.site, Fault: s.state.Cause.Fault().String(), Cause: s.state.Cause,
				Injected: s.state.Injected, AlertID: a.AlertID, BundleID: a.BundleID, ObservedAt: a.ObservedAt,
			})
		}
		out = append(out, a)
	}
	return out
}

// collectAlertBundle collects a log bundle for a fault unless one was
// collected within the cooldown. It returns the bundle ID, or "" if none.
func (d *Device) collectAlertBundle(now time.Time) string {
	if d.uploader == nil {
		return ""
	}
	if !d.lastAlertBundle.IsZero() && now.Sub(d.lastAlertBundle) < d.bundleCfg.AlertCooldown {
		return ""
	}
	d.lastAlertBundle = now
	return d.collect(now, wire.ReasonAlert)
}

// sendAlerts delivers queued alerts oldest first. It runs before telemetry
// each tick: an alert is small and is what opens a support case, so it
// shouldn't wait behind a telemetry backlog after an outage.
func (d *Device) sendAlerts(ctx context.Context) {
	for i := 0; i < maxAlertsPerTick; i++ {
		a, ok := d.alerts.peek()
		if !ok {
			return
		}
		actx, span := platform.Tracer().Start(ctx, "send alert", trace.WithAttributes(
			attribute.String("device.id", d.id),
			attribute.String("site.id", d.site),
			attribute.String("alert.id", a.AlertID),
			attribute.String("alert.code", string(a.Code)),
		))
		err := d.client.SendAlert(actx, d.id, d.Token(), a)
		platform.RecordError(actx, err)
		span.End()
		switch {
		case err == nil:
			d.alerts.pop()
			d.m.alertsSent.Inc()
			continue
		case ctx.Err() != nil:
			return
		case errors.Is(err, ErrUnauthorized):
			d.m.alertSendFailures.WithLabelValues("unauthorized").Inc()
			d.register(ctx) // the alert is retried on the next tick
		case endpointMissing(err):
			// The cloud (or gateway) predates alerts. Keep them: an agent
			// upgraded before the cloud must not throw its faults away.
			d.m.alertSendFailures.WithLabelValues("endpoint_missing").Inc()
			d.log.Debug("alerts endpoint not available yet", "err", err)
		case isPermanent(err):
			d.alerts.pop()
			d.m.alertsRejected.Inc()
			d.log.ErrorContext(actx, "backend rejected alert permanently, dropping it", "alert_id", a.AlertID, "code", a.Code, "err", err)
			d.logs.Add(time.Now(), "ERROR", "support", fmt.Sprintf("alert %s rejected by cloud: %v", a.AlertID, err))
			continue
		default:
			reason := errorReason(err)
			d.m.alertSendFailures.WithLabelValues(reason).Inc()
			d.log.WarnContext(actx, "alert send failed, will retry next tick", "alert_id", a.AlertID, "reason", reason, "err", err)
		}
		return
	}
}

func endpointMissing(err error) bool {
	var se *StatusError
	return errors.As(err, &se) && (se.Code == http.StatusNotFound || se.Code == http.StatusMethodNotAllowed)
}
