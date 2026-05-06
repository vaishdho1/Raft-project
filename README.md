# Raft-Based Replicated Key-Value Store

A fault-tolerant replicated key-value store built on the Raft consensus protocol in Go. Implements leader election, log replication, crash-safe persistence, snapshot compaction, and linearizable state-machine replication.

For the full design write-up, see [Agreeing Under Chaos](https://vaishdho1.github.io/my-portfolio/raft-consensus-protocol.html).

## What it implements

- **Raft consensus:** leader election, heartbeats, `RequestVote`, `AppendEntries`, quorum commit, and in-order apply
- **Crash-safe persistence:** durable state with disk and memory modes
- **Snapshot compaction:** local snapshots and `InstallSnapshot` for lagging followers
- **Linearizable KV store:** `Get` and `Put` with version-gated OCC and `ErrMaybe` retry semantics
- **Observability:** pluggable Prometheus metrics sink with Grafana dashboards

## Correctness cases handled

| Case | Problem | Handling |
|---|---|---|
| Leader failover | Leader crashes after replicating but before replying | Clients retry with the same version and OCC prevents double-apply |
| Network partition | Both sides may try to make progress | Only a majority partition can elect a leader and commit |
| Stale leader rejoins | Old leader tries to append after losing authority | Term checks force step-down |
| Log divergence | Followers have conflicting uncommitted entries | Fast conflict repair backtracks by term |
| Lagging follower | Follower falls behind after compaction | `InstallSnapshot` restores valid state |
| Server crash | Node restarts after losing volatile state | Persistent state is restored on restart |
| Client retry ambiguity | Client does not know if a timed-out write committed | Server returns `ErrMaybe` and clients retry safely using versions |

## Key design decisions

## Key design decisions

- **Per-follower replication loops:** each follower has a long-running replication loop instead of creating a new goroutine for every retry. This keeps concurrency bounded when the network is unreliable.

- **Fast log repair:** when a follower rejects an append, it returns conflict information so the leader can jump back by term instead of scanning back one log entry at a time.

- **Crash-safe persistence:** uses synchronous persistence for correctness-critical Raft state changes and asynchronous persistence for lower-priority writes such as snapshots.

- **Retry-safe writes:** writes use client-provided versions, so retrying a timed-out request does not accidentally apply the same logical update twice.

## Evaluation

Evaluated on 5-node clusters under leader churn, crashes, partitions, message loss, and unreliable networks. Correctness validated with Porcupine linearizability checking.

| Metric | Result |
|---|---:|
| Throughput | ~1K ops/sec |
| p99 latency | ~100ms |
| Linearizability violations | 0 |

Observability via Prometheus and Grafana dashboards under `deploy/monitoring/`. Scenario drivers run fault-injection workloads with live metrics.

## Attribution

Built on the MIT 6.5840 distributed systems lab framework.

I implemented the Raft core, persistence, snapshotting, KV server/client logic, OCC retry semantics, Prometheus/Grafana monitoring, and scenario drivers.

The RPC simulation layer, serialization helpers, and tester harness were course-provided.

## Future work

- Replace simulated RPC with gRPC
- Add WAL-based persistence and log compaction improvements
- Add read-index or lease-based optimized reads

## References

- [Raft paper](https://raft.github.io/)
- [Agreeing Under Chaos](https://vaishdho1.github.io/my-portfolio/raft-consensus-protocol.html)