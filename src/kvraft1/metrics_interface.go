package kvraft

import "time"

type MetricsSink interface {
	// Histogram: Records how long a request took
	ObserveLatency(method string, duration time.Duration)
	// Histogram: Records server-side Submit() latency by outcome
	ObserveSubmitLatency(method string, status string, reason string, duration time.Duration)
	// Counter: Records errors or retries
	IncRetry(method string)
	// Counter: Records requests
	IncRequest(method string)
	// Counter: Records failures by reason
	IncFailure(method string, reason string)
	// Gauge: Records current version seen (for linearizability check)
	SetObservedVersion(method string, version int64)
	// Gauge: Records how many ops are waiting in Raft
	SetPendingOps(count int)
	// Gauge: Records client in-flight operations
	SetClientInFlight(count int)
	// Counter: Records applied operations
	IncOpsApplied(method string)
	// Counter: Records operation errors
	IncOpError(method string, reason string)
	// Gauge: Records current number of entries in KV state
	SetStateEntries(count int)
	// Gauge: Records snapshot size in bytes
	SetSnapshotBytes(size int)
	// Counter: Records snapshots created
	IncSnapshot()
	// Gauge: Records apply queue depth
	SetApplyQueueDepth(depth int)
}

// A no-op implementation for tests that don't care
type NoopMetrics struct{}

func (n NoopMetrics) ObserveLatency(s string, d time.Duration) {}
func (n NoopMetrics) ObserveSubmitLatency(m string, st string, r string, d time.Duration) {
}
func (n NoopMetrics) IncRetry(s string)                    {}
func (n NoopMetrics) IncRequest(s string)                  {}
func (n NoopMetrics) IncFailure(s string, r string)        {}
func (n NoopMetrics) SetObservedVersion(s string, v int64) {}
func (n NoopMetrics) SetPendingOps(c int)                  {}
func (n NoopMetrics) SetClientInFlight(c int)              {}
func (n NoopMetrics) IncOpsApplied(s string)               {}
func (n NoopMetrics) IncOpError(s string, e string)        {}
func (n NoopMetrics) SetStateEntries(c int)                {}
func (n NoopMetrics) SetSnapshotBytes(s int)               {}
func (n NoopMetrics) IncSnapshot()                         {}
func (n NoopMetrics) SetApplyQueueDepth(d int)             {}

var metricsSink MetricsSink = NoopMetrics{}

func SetMetricsSink(s MetricsSink) {
	if s == nil {
		metricsSink = NoopMetrics{}
		return
	}
	metricsSink = s
}

func getMetricsSink() MetricsSink {
	return metricsSink
}

func metricsEnabled() bool {
	_, ok := metricsSink.(NoopMetrics)
	return !ok
}
