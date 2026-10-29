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

	alertsRaised      *prometheus.CounterVec
	alertsSent        prometheus.Counter
	alertsRejected    prometheus.Counter
	alertsDropped     prometheus.Counter
	alertSendFailures *prometheus.CounterVec

	bundlesCreated       *prometheus.CounterVec
	bundleUploads        *prometheus.CounterVec
	bundleUploadFailures *prometheus.CounterVec
	bundleBytes          prometheus.Counter
	chunkDuration        prometheus.Histogram
	bundleUploadDuration prometheus.Histogram
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

		alertsRaised: f.NewCounterVec(prometheus.CounterOpts{
			Name: "connectivity_agent_alerts_raised_total",
			Help: "Faults detected and queued as alerts, by code.",
		}, []string{"code"}),
		alertsSent: f.NewCounter(prometheus.CounterOpts{
			Name: "connectivity_agent_alerts_sent_total",
			Help: "Alerts acknowledged by the cloud or the gateway.",
		}),
		alertsRejected: f.NewCounter(prometheus.CounterOpts{
			Name: "connectivity_agent_alerts_rejected_total",
			Help: "Alerts the backend refused permanently (4xx) and the agent dropped.",
		}),
		alertsDropped: f.NewCounter(prometheus.CounterOpts{
			Name: "connectivity_agent_alerts_dropped_total",
			Help: "Alerts discarded because the outbox was full. These faults never reach support.",
		}),
		alertSendFailures: f.NewCounterVec(prometheus.CounterOpts{
			Name: "connectivity_agent_alert_send_failures_total",
			Help: "Failed alert sends that will be retried, by reason.",
		}, []string{"reason"}),

		bundlesCreated: f.NewCounterVec(prometheus.CounterOpts{
			Name: "connectivity_agent_bundles_created_total",
			Help: "Log bundles collected and spooled, by reason.",
		}, []string{"reason"}),
		bundleUploads: f.NewCounterVec(prometheus.CounterOpts{
			Name: "connectivity_agent_bundle_uploads_total",
			Help: "Bundles that left the spool, by result (completed, rejected, evicted).",
		}, []string{"result"}),
		bundleUploadFailures: f.NewCounterVec(prometheus.CounterOpts{
			Name: "connectivity_agent_bundle_upload_failures_total",
			Help: "Upload attempts that failed and will be retried, by reason.",
		}, []string{"reason"}),
		bundleBytes: f.NewCounter(prometheus.CounterOpts{
			Name: "connectivity_agent_bundle_bytes_uploaded_total",
			Help: "Bundle bytes accepted by the cloud, counted per chunk.",
		}),
		chunkDuration: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "connectivity_agent_bundle_chunk_duration_seconds",
			Help:    "Time to upload one bundle chunk.",
			Buckets: []float64{.01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
		}),
		bundleUploadDuration: f.NewHistogram(prometheus.HistogramOpts{
			Name:    "connectivity_agent_bundle_upload_duration_seconds",
			Help:    "Time from init to complete for a successful upload attempt.",
			Buckets: []float64{.1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120},
		}),
	}
}
