package raft

import "time"

// This file wraps the sink and the type of transport used
// to check delays
type MetricsTransport struct {
	from  int
	inner Transport
	sink  MetricsSink
}

func NewMetricsTransport(from int, inner Transport, sink MetricsSink) *MetricsTransport {
	if sink == nil {
		sink = NoopSink{}
	}
	return &MetricsTransport{from: from, inner: inner, sink: sink}
}

func (tr *MetricsTransport) NumPeers() int {
	return tr.inner.NumPeers()
}

func (tr *MetricsTransport) observe(method string, to int, fn func() bool) bool {
	start := time.Now()
	ok := fn()
	tr.sink.ObserveRPC(method, tr.from, to, ok, time.Since(start))
	return ok
}
func (tr *MetricsTransport) AppendEntries(peer int, args *AppendEntriesReqArgs, reply *AppendEntriesResponseArgs) bool {
	rpc := func() bool {
		return tr.inner.AppendEntries(peer, args, reply)
	}
	// Observe batch size: number of log entries being sent in this AppendEntries.
	tr.sink.ObserveBatchSize(tr.from, "append_entries_entries", len(args.Entries))
	return tr.observe("AppendEntries", peer, rpc)
}
func (tr *MetricsTransport) RequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	rpc := func() bool {
		return tr.inner.RequestVote(peer, args, reply)
	}
	return tr.observe("RequestVote", peer, rpc)
}
func (tr *MetricsTransport) InstallSnapshot(peer int, args *SnapshotRequestArgs, reply *SnapshotReplyArgs) bool {
	rpc := func() bool {
		return tr.inner.InstallSnapshot(peer, args, reply)
	}
	return tr.observe("InstallSnapshot", peer, rpc)
}
