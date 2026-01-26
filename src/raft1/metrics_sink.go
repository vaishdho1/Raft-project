package raft

import (
	"time"
)

type MetricsSink interface {
	ObserveRPC(method string, from int, to int, ok bool, dur time.Duration)
	// Server-side handler durations (includes lock wait + processing + any synchronous persistence).
	ObserveAppendEntriesHandlerDuration(me int, dur time.Duration)
	// Leader commit latency from Start() to commit.
	ObserveCommitLatency(me int, dur time.Duration)
	// Persistence queue timings for persistSynchronously().
	ObservePersistEnqueueWait(me int, dur time.Duration)
	ObservePersistDoneWait(me int, dur time.Duration)
	SetTerm(me int, term int)
	SetRole(me int, role RaftState)
	IncElectionStarted(me int)
	IncLeaderElected(me int)
	ObserveElectionDuration(me int, dur time.Duration)
	SetCommitIndex(me int, v int)
	SetLastApplied(me int, v int)
	SetReplicationLag(me int, peerId int, lag int)
	// Log/snapshot/persistence metrics.
	SetStartIndex(me int, v int)
	SetLogLen(me int, v int)
	SetLogSizeBytes(me int, v int)
	SetRaftStateBytes(me int, v int)
	SetSnapshotIndex(me int, v int)
	SetSnapshotBytes(me int, v int)
	SetSnapshotSizeBytes(me int, v int)
	ObservePersistDuration(me int, dur time.Duration)
	ObserveSnapshotDuration(me int, kind string, dur time.Duration)
	IncDiskWrites(me int, kind string)
	// In-progress gauges (use +1 at start, defer -1 at end).
	AddSnapshotInProgress(me int, kind string, delta int)
	// Runtime/queue instrumentation.
	SetChannelDepth(me int, name string, depth int)
	ObserveBatchSize(me int, typ string, n int)
}
type NoopSink struct{}

func (NoopSink) ObserveRPC(method string, from int, to int, ok bool, dur time.Duration) {}
func (NoopSink) ObserveAppendEntriesHandlerDuration(me int, dur time.Duration)          {}
func (NoopSink) ObserveCommitLatency(me int, dur time.Duration)                         {}
func (NoopSink) ObservePersistEnqueueWait(me int, dur time.Duration)                    {}
func (NoopSink) ObservePersistDoneWait(me int, dur time.Duration)                       {}
func (NoopSink) SetTerm(me int, term int)                                               {}
func (NoopSink) SetRole(me int, role RaftState)                                         {}
func (NoopSink) IncElectionStarted(me int)                                              {}
func (NoopSink) IncLeaderElected(me int)                                                {}
func (NoopSink) ObserveElectionDuration(me int, dur time.Duration)                      {}
func (NoopSink) SetCommitIndex(me int, v int)                                           {}
func (NoopSink) SetLastApplied(me int, v int)                                           {}
func (NoopSink) SetReplicationLag(me int, peerId int, lag int)                          {}
func (NoopSink) SetStartIndex(me int, v int)                                            {}
func (NoopSink) SetLogLen(me int, v int)                                                {}
func (NoopSink) SetLogSizeBytes(me int, v int)                                          {}
func (NoopSink) SetRaftStateBytes(me int, v int)                                        {}
func (NoopSink) SetSnapshotIndex(me int, v int)                                         {}
func (NoopSink) SetSnapshotBytes(me int, v int)                                         {}
func (NoopSink) SetSnapshotSizeBytes(me int, v int)                                     {}
func (NoopSink) ObservePersistDuration(me int, dur time.Duration)                       {}
func (NoopSink) ObserveSnapshotDuration(me int, kind string, dur time.Duration)         {}
func (NoopSink) IncDiskWrites(me int, kind string)                                      {}
func (NoopSink) AddSnapshotInProgress(me int, kind string, delta int)                   {}
func (NoopSink) SetChannelDepth(me int, name string, depth int)                         {}
func (NoopSink) ObserveBatchSize(me int, typ string, n int)                             {}
