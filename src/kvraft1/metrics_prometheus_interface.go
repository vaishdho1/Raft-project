package kvraft

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type PromSink struct {
	reg *prometheus.Registry

	observeLatency    *prometheus.HistogramVec
	submitLatency     *prometheus.HistogramVec
	requestTotal      *prometheus.CounterVec
	retryTotal        *prometheus.CounterVec
	failureTotal      *prometheus.CounterVec
	clientInFlight    prometheus.Gauge
	observedVersion   *prometheus.GaugeVec
	pendingOpsGauge   prometheus.Gauge
	opsTotal          *prometheus.CounterVec
	stateEntriesGauge prometheus.Gauge
	snapshotBytes     prometheus.Gauge
	snapshotTotal     prometheus.Counter
	applyQueueDepth   prometheus.Gauge
	queueDepth        prometheus.Gauge
}

var _ MetricsSink = (*PromSink)(nil)

func NewPromSink() *PromSink {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)
	return NewPromSinkWithRegistry(reg)
}

func NewPromSinkWithRegistry(reg *prometheus.Registry) *PromSink {
	latencyBuckets := []float64{
		0.00025, 0.0005, 0.001, 0.002, 0.005,
		0.01, 0.02, 0.05, 0.1,
		0.2, 0.5, 1.0, 2.0,
		3.0, 5.0, 10.0,
	}

	observeLatency := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kv_request_latency_seconds",
			Help:    "Client request latency by method",
			Buckets: latencyBuckets,
		},
		[]string{"method"},
	)
	submitLatency := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "kv_submit_latency_seconds",
			Help:    "Server-side Submit() latency by method and outcome",
			Buckets: latencyBuckets,
		},
		[]string{"method", "status", "reason"},
	)

	requestTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kv_client_requests_total",
			Help: "Total client requests by method",
		},
		[]string{"method"},
	)

	retryTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kv_client_retries_total",
			Help: "Total client retries by method",
		},
		[]string{"method"},
	)

	failureTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kv_client_failures_total",
			Help: "Total client failures by method and reason",
		},
		[]string{"method", "reason"},
	)

	clientInFlight := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kv_client_in_flight",
			Help: "Current in-flight client requests",
		},
	)

	observedVersion := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kv_observed_version",
			Help: "Last observed version per method",
		},
		[]string{"method"},
	)

	pendingOps := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kv_pending_ops",
			Help: "Current number of pending operations",
		},
	)

	opsTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kv_ops_total",
			Help: "Total number of operations executed by the state machine",
		},
		[]string{"method", "status", "reason"},
	)

	stateEntriesGauge := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kv_state_entries",
			Help: "Number of entries in KV state",
		},
	)

	snapshotBytes := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kv_snapshot_bytes",
			Help: "Last snapshot size in bytes",
		},
	)

	snapshotTotal := prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "kv_snapshot_total",
			Help: "Total snapshots created",
		},
	)

	applyQueueDepth := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kv_apply_queue_depth",
			Help: "Apply channel depth",
		},
	)
	queueDepth := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "kv_queue_depth",
			Help: "Apply channel depth (alias)",
		},
	)

	reg.MustRegister(
		observeLatency,
		submitLatency,
		requestTotal,
		retryTotal,
		failureTotal,
		clientInFlight,
		observedVersion,
		pendingOps,
		opsTotal,
		stateEntriesGauge,
		snapshotBytes,
		snapshotTotal,
		applyQueueDepth,
		queueDepth,
	)

	return &PromSink{
		reg:               reg,
		observeLatency:    observeLatency,
		submitLatency:     submitLatency,
		requestTotal:      requestTotal,
		retryTotal:        retryTotal,
		failureTotal:      failureTotal,
		clientInFlight:    clientInFlight,
		observedVersion:   observedVersion,
		pendingOpsGauge:   pendingOps,
		opsTotal:          opsTotal,
		stateEntriesGauge: stateEntriesGauge,
		snapshotBytes:     snapshotBytes,
		snapshotTotal:     snapshotTotal,
		applyQueueDepth:   applyQueueDepth,
		queueDepth:        queueDepth,
	}
}

func (s *PromSink) Registry() *prometheus.Registry {
	return s.reg
}

func (s *PromSink) ObserveLatency(method string, duration time.Duration) {
	s.observeLatency.WithLabelValues(method).Observe(duration.Seconds())
}

func (s *PromSink) ObserveSubmitLatency(method string, status string, reason string, duration time.Duration) {
	s.submitLatency.WithLabelValues(method, status, reason).Observe(duration.Seconds())
}

func (s *PromSink) IncRetry(method string) {
	s.retryTotal.WithLabelValues(method).Inc()
}

func (s *PromSink) IncRequest(method string) {
	s.requestTotal.WithLabelValues(method).Inc()
}

func (s *PromSink) IncFailure(method string, reason string) {
	s.failureTotal.WithLabelValues(method, reason).Inc()
}

func (s *PromSink) SetObservedVersion(method string, version int64) {
	s.observedVersion.WithLabelValues(method).Set(float64(version))
}

func (s *PromSink) SetPendingOps(count int) {
	s.pendingOpsGauge.Set(float64(count))
}

func (s *PromSink) SetClientInFlight(count int) {
	s.clientInFlight.Set(float64(count))
}

func (s *PromSink) IncOpsApplied(method string) {
	s.opsTotal.WithLabelValues(method, "ok", "none").Inc()
}

func (s *PromSink) IncOpError(method string, reason string) {
	s.opsTotal.WithLabelValues(method, "err", reason).Inc()
}

func (s *PromSink) SetStateEntries(count int) {
	s.stateEntriesGauge.Set(float64(count))
}

func (s *PromSink) SetSnapshotBytes(size int) {
	s.snapshotBytes.Set(float64(size))
}

func (s *PromSink) IncSnapshot() {
	s.snapshotTotal.Inc()
}

func (s *PromSink) SetApplyQueueDepth(depth int) {
	s.applyQueueDepth.Set(float64(depth))
	s.queueDepth.Set(float64(depth))
}
