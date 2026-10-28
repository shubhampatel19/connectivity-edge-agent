package agent

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type metrics struct {
	registrations     prometheus.Counter
	registerFailures  *prometheus.CounterVec
	batchesSent       prometheus.Counter
	batchesRejected   prometheus.Counter
	samplesSent       prometheus.Counter
	samplesDropped    prometheus.Counter
	sendFailures      *prometheus.CounterVec
	sendDuration      prometheus.Histogram
	heartbeatFailures *prometheus.CounterVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	f := promauto.With(reg)
	return &metrics{
		registrations: f.NewCounter(prometheus.CounterOpts{
			Name: "connectivity_agent_registrations_total",
			Help: "Successful device registrations, including re-registrations.",
		}),
		registerFailures: f.NewCounterVec(prometheus.CounterOpts{
			Name: "connectivity_agent_register_failures_total",
			Help: "Failed registration attempts, by reason.",
		}, []string{"reason"}),
		batchesSent: f.NewCounter(prometheus.CounterOpts{
			Name: "connectivity_agent_batches_sent_total",
			Help: "Telemetry batches acknowledged by the backend.",
		}),
		batchesRejected: f.NewCounter(prometheus.CounterOpts{
			Name: "connectivity_agent_batches_rejected_total",
			Help: "Batches the backend refused permanently (4xx) and the agent dropped.",
		}),
		samplesSent: f.NewCounter(prometheus.CounterOpts{
			Name: "connectivity_agent_samples_sent_total",
			Help: "Samples acknowledged by the backend.",
		}),
		samplesDropped: f.NewCounter(prometheus.CounterOpts{
			Name: "connectivity_agent_samples_dropped_total",
			Help: "Samples overwritten because the local buffer was full.",
		}),
		sendFailures: f.NewCounterVec(prometheus.CounterOpts{
			Name: "connectivity_agent_send_failures_total",
			Help: "Failed telemetry sends that will be retried, by reason.",
		}, []string{"reason"}),
		sendDuration: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "connectivity_agent_send_duration_seconds",
			Help:    "Round-trip time of telemetry sends, successful or not.",
			Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}),
		heartbeatFailures: f.NewCounterVec(prometheus.CounterOpts{
			Name: "connectivity_agent_heartbeat_failures_total",
			Help: "Failed heartbeats, by reason.",
		}, []string{"reason"}),
	}
}
