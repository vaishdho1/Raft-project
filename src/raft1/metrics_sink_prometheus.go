//go:build prometheus
// +build prometheus

package raft

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// PromSink is a Prometheus-backed MetricsSink.
// Build with: -tags=prometheus
//
// This keeps raft's core build/test path free of Prometheus dependencies unless explicitly enabled.
type PromSink struct {
	reg *prometheus.Registry

	rpcTotal                     *prometheus.CounterVec
	rpcDuration                  *prometheus.HistogramVec
	appendEntriesHandlerDuration *prometheus.HistogramVec
	commitLatency                *prometheus.HistogramVec
	electionDuration             *prometheus.HistogramVec
	persistEnqueueWait           *prometheus.HistogramVec
	persistDoneWait              *prometheus.HistogramVec
	channelDepth                 *prometheus.GaugeVec
	batchSize                    *prometheus.HistogramVec
	roleGauge                    *prometheus.GaugeVec
	termGauge                    *prometheus.GaugeVec

	electionsTotal *prometheus.CounterVec
	leadersTotal   *prometheus.CounterVec

	commitIndex *prometheus.GaugeVec
	lastApplied *prometheus.GaugeVec

	replicationLag *prometheus.GaugeVec

	startIndex         *prometheus.GaugeVec
	logLen             *prometheus.GaugeVec
	logSizeBytes       *prometheus.GaugeVec
	raftStateBytes     *prometheus.GaugeVec
	snapshotIndex      *prometheus.GaugeVec
	snapshotBytes      *prometheus.GaugeVec
	snapshotSizeBytes  *prometheus.GaugeVec
	snapshotInProgress *prometheus.GaugeVec

	persistDuration  *prometheus.HistogramVec
	snapshotDuration *prometheus.HistogramVec
	diskWritesTotal  *prometheus.CounterVec
}

var _ MetricsSink = (*PromSink)(nil)

func NewPromSink() *PromSink {
	reg := prometheus.NewRegistry()
	// Expose Go runtime + process metrics into this registry.
	reg.MustRegister(
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)
	return NewPromSinkWithRegistry(reg)
}

func NewPromSinkWithRegistry(reg *prometheus.Registry) *PromSink {
	// Histogram buckets tuned for Raft:
	// - RPCs are usually sub-ms to 10s of ms, but can spike under delays/partitions.
	// - Persist is usually ~1-20ms locally, but can spike to 100s ms under pressure.
	// - Snapshots can range from a few ms to seconds depending on size and disk.
	rpcBuckets := []float64{
		0.00025, 0.0005, 0.001, 0.002, 0.005,
		0.01, 0.02, 0.05, 0.1,
		0.2, 0.5, 1.0, 2.0,
		3.0, 5.0, 10.0,
	}
	persistBuckets := []float64{
		0.0005, 0.001, 0.002, 0.005,
		0.0075, 0.01, 0.015, 0.02, 0.03, 0.04, 0.05, 0.06, 0.08, 0.1,
		0.2, 0.5, 1.0,
	}
	snapshotBuckets := []float64{
		0.001, 0.002, 0.005,
		0.0075, 0.01, 0.015, 0.02, 0.03, 0.04, 0.05, 0.06, 0.08, 0.1,
		0.2, 0.5, 1.0, 2.0, 5.0,
	}

	rpcTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "raft_rpc_total",
			Help: "Total outbound Raft RPCs by method/from/to and transport ok",
		},
		[]string{"method", "from", "to", "ok"},
	)

	rpcDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "raft_rpc_duration_seconds",
			Help:    "Outbound Raft RPC latency in seconds",
			Buckets: rpcBuckets,
		},
		[]string{"method", "from", "to", "ok"},
	)

	appendEntriesHandlerDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "raft_appendentries_handler_duration_seconds",
			Help:    "Total time spent handling AppendEntries RPC on the receiver (includes lock wait and synchronous persistence)",
			Buckets: rpcBuckets,
		},
		[]string{"node"},
	)
	commitLatency := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "raft_commit_latency_seconds",
			Help:    "Leader commit latency from Start() to commit",
			Buckets: rpcBuckets,
		},
		[]string{"node"},
	)
	electionDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "raft_election_duration_seconds",
			Help:    "Time from election start to leader elected",
			Buckets: rpcBuckets,
		},
		[]string{"node"},
	)

	persistEnqueueWait := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "raft_persist_enqueue_wait_seconds",
			Help:    "Time blocked sending to persistWaitch in persistSynchronously()",
			Buckets: rpcBuckets,
		},
		[]string{"node"},
	)

	persistDoneWait := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "raft_persist_done_wait_seconds",
			Help:    "Time blocked waiting for doneCh in persistSynchronously()",
			Buckets: rpcBuckets,
		},
		[]string{"node"},
	)

	channelDepth := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "raft_channel_depth", Help: "Channel depth (len) by node/name"},
		[]string{"node", "name"},
	)

	batchSize := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "raft_batch_size",
			Help:    "Batch sizes by node/type (e.g., apply batch size, append entries per RPC)",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024},
		},
		[]string{"node", "type"},
	)

	roleGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "raft_role", Help: "Raft node role (1 for active role)"},
		[]string{"node", "role"},
	)

	termGauge := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "raft_current_term", Help: "Current Raft term"},
		[]string{"node"},
	)

	electionsTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "raft_elections_started_total", Help: "Elections started"},
		[]string{"node"},
	)

	leadersTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "raft_leader_elected_total", Help: "Times this node became leader"},
		[]string{"node"},
	)

	commitIndex := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "raft_commit_index", Help: "Commit index"},
		[]string{"node"},
	)

	lastApplied := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "raft_last_applied", Help: "Last applied index"},
		[]string{"node"},
	)

	replicationLag := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "raft_replication_lag_entries", Help: "Leader's replication lag to peer in log entries"},
		[]string{"leader", "peer"},
	)

	startIndex := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "raft_start_index", Help: "Log start index (snapshot index / base)"},
		[]string{"node"},
	)

	logLen := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "raft_log_len", Help: "Number of log entries kept in memory (excluding snapshot base)"},
		[]string{"node"},
	)

	logSizeBytes := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "raft_log_size_bytes", Help: "Approximate in-memory log size in bytes"},
		[]string{"node"},
	)

	raftStateBytes := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "raft_raftstate_bytes", Help: "Persisted raftstate size in bytes"},
		[]string{"node"},
	)

	snapshotIndex := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "raft_snapshot_index", Help: "Snapshot base index (last included index)"},
		[]string{"node"},
	)

	snapshotBytes := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "raft_snapshot_bytes", Help: "In-memory snapshot size in bytes"},
		[]string{"node"},
	)

	snapshotSizeBytes := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "raft_snapshot_size_bytes", Help: "Snapshot size in bytes (persisted/serialized)"},
		[]string{"node"},
	)

	snapshotInProgress := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "raft_snapshot_in_progress", Help: "Number of snapshots currently in progress"},
		[]string{"node", "kind"},
	)

	persistDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "raft_persist_duration_seconds",
			Help:    "Time spent persisting Raft state to stable storage",
			Buckets: persistBuckets,
		},
		[]string{"node"},
	)

	snapshotDuration := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "raft_snapshot_duration_seconds",
			Help:    "Time spent creating/installing snapshots",
			Buckets: snapshotBuckets,
		},
		[]string{"node", "kind"},
	)

	diskWritesTotal := prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "raft_disk_writes_total", Help: "Disk write events by kind"},
		[]string{"node", "kind"},
	)

	reg.MustRegister(
		rpcTotal, rpcDuration,
		appendEntriesHandlerDuration,
		commitLatency,
		electionDuration,
		persistEnqueueWait, persistDoneWait,
		channelDepth,
		batchSize,
		roleGauge, termGauge,
		electionsTotal, leadersTotal,
		commitIndex, lastApplied,
		replicationLag,
		startIndex, logLen, logSizeBytes,
		raftStateBytes,
		snapshotIndex, snapshotBytes, snapshotSizeBytes, snapshotInProgress,
		persistDuration, snapshotDuration,
		diskWritesTotal,
	)

	return &PromSink{
		reg:                          reg,
		rpcTotal:                     rpcTotal,
		rpcDuration:                  rpcDuration,
		appendEntriesHandlerDuration: appendEntriesHandlerDuration,
		commitLatency:                commitLatency,
		electionDuration:             electionDuration,
		persistEnqueueWait:           persistEnqueueWait,
		persistDoneWait:              persistDoneWait,
		channelDepth:                 channelDepth,
		batchSize:                    batchSize,
		roleGauge:                    roleGauge,
		termGauge:                    termGauge,
		electionsTotal:               electionsTotal,
		leadersTotal:                 leadersTotal,
		commitIndex:                  commitIndex,
		lastApplied:                  lastApplied,
		replicationLag:               replicationLag,
		startIndex:                   startIndex,
		logLen:                       logLen,
		logSizeBytes:                 logSizeBytes,
		raftStateBytes:               raftStateBytes,
		snapshotIndex:                snapshotIndex,
		snapshotBytes:                snapshotBytes,
		snapshotSizeBytes:            snapshotSizeBytes,
		snapshotInProgress:           snapshotInProgress,
		persistDuration:              persistDuration,
		snapshotDuration:             snapshotDuration,
		diskWritesTotal:              diskWritesTotal,
	}
}

func (s *PromSink) Registry() *prometheus.Registry {
	return s.reg
}

func (s *PromSink) ObserveRPC(method string, from int, to int, ok bool, dur time.Duration) {
	fromS := strconv.Itoa(from)
	toS := strconv.Itoa(to)
	okS := strconv.FormatBool(ok)
	s.rpcTotal.WithLabelValues(method, fromS, toS, okS).Inc()
	s.rpcDuration.WithLabelValues(method, fromS, toS, okS).Observe(dur.Seconds())
}

func (s *PromSink) ObserveAppendEntriesHandlerDuration(me int, dur time.Duration) {
	s.appendEntriesHandlerDuration.WithLabelValues(strconv.Itoa(me)).Observe(dur.Seconds())
}

func (s *PromSink) ObserveCommitLatency(me int, dur time.Duration) {
	s.commitLatency.WithLabelValues(strconv.Itoa(me)).Observe(dur.Seconds())
}

func (s *PromSink) ObserveElectionDuration(me int, dur time.Duration) {
	s.electionDuration.WithLabelValues(strconv.Itoa(me)).Observe(dur.Seconds())
}

func (s *PromSink) ObservePersistEnqueueWait(me int, dur time.Duration) {
	s.persistEnqueueWait.WithLabelValues(strconv.Itoa(me)).Observe(dur.Seconds())
}

func (s *PromSink) ObservePersistDoneWait(me int, dur time.Duration) {
	s.persistDoneWait.WithLabelValues(strconv.Itoa(me)).Observe(dur.Seconds())
}

func (s *PromSink) SetChannelDepth(me int, name string, depth int) {
	s.channelDepth.WithLabelValues(strconv.Itoa(me), name).Set(float64(depth))
}

func (s *PromSink) ObserveBatchSize(me int, typ string, n int) {
	s.batchSize.WithLabelValues(strconv.Itoa(me), typ).Observe(float64(n))
}

func (s *PromSink) SetRole(me int, role RaftState) {
	n := strconv.Itoa(me)
	// Set the active role to 1 and others to 0 (cheap and clear).
	s.roleGauge.WithLabelValues(n, "follower").Set(boolToFloat(role == StateFollower))
	s.roleGauge.WithLabelValues(n, "candidate").Set(boolToFloat(role == StateCandidate))
	s.roleGauge.WithLabelValues(n, "leader").Set(boolToFloat(role == StateLeader))
}

func (s *PromSink) SetTerm(me int, term int) {
	s.termGauge.WithLabelValues(strconv.Itoa(me)).Set(float64(term))
}

func (s *PromSink) IncElectionStarted(me int) {
	s.electionsTotal.WithLabelValues(strconv.Itoa(me)).Inc()
}

func (s *PromSink) IncLeaderElected(me int) {
	s.leadersTotal.WithLabelValues(strconv.Itoa(me)).Inc()
}

func (s *PromSink) SetCommitIndex(me int, v int) {
	s.commitIndex.WithLabelValues(strconv.Itoa(me)).Set(float64(v))
}

func (s *PromSink) SetLastApplied(me int, v int) {
	s.lastApplied.WithLabelValues(strconv.Itoa(me)).Set(float64(v))
}

func (s *PromSink) SetReplicationLag(me int, peer int, lag int) {
	s.replicationLag.WithLabelValues(strconv.Itoa(me), strconv.Itoa(peer)).Set(float64(lag))
}

func (s *PromSink) SetStartIndex(me int, v int) {
	s.startIndex.WithLabelValues(strconv.Itoa(me)).Set(float64(v))
}

func (s *PromSink) SetLogLen(me int, v int) {
	s.logLen.WithLabelValues(strconv.Itoa(me)).Set(float64(v))
}

func (s *PromSink) SetLogSizeBytes(me int, v int) {
	s.logSizeBytes.WithLabelValues(strconv.Itoa(me)).Set(float64(v))
}

func (s *PromSink) SetRaftStateBytes(me int, v int) {
	s.raftStateBytes.WithLabelValues(strconv.Itoa(me)).Set(float64(v))
}

func (s *PromSink) SetSnapshotIndex(me int, v int) {
	s.snapshotIndex.WithLabelValues(strconv.Itoa(me)).Set(float64(v))
}

func (s *PromSink) SetSnapshotBytes(me int, v int) {
	s.snapshotBytes.WithLabelValues(strconv.Itoa(me)).Set(float64(v))
}

func (s *PromSink) SetSnapshotSizeBytes(me int, v int) {
	s.snapshotSizeBytes.WithLabelValues(strconv.Itoa(me)).Set(float64(v))
}

func (s *PromSink) AddSnapshotInProgress(me int, kind string, delta int) {
	s.snapshotInProgress.WithLabelValues(strconv.Itoa(me), kind).Add(float64(delta))
}

func (s *PromSink) ObservePersistDuration(me int, dur time.Duration) {
	s.persistDuration.WithLabelValues(strconv.Itoa(me)).Observe(dur.Seconds())
}

func (s *PromSink) ObserveSnapshotDuration(me int, kind string, dur time.Duration) {
	s.snapshotDuration.WithLabelValues(strconv.Itoa(me), kind).Observe(dur.Seconds())
}

func (s *PromSink) IncDiskWrites(me int, kind string) {
	s.diskWritesTotal.WithLabelValues(strconv.Itoa(me), kind).Inc()
}

func boolToFloat(b bool) float64 {
	if b {
		return 1.0
	}
	return 0.0
}
