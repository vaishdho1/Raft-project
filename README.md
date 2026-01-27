# Raft Consensus Protocol
This project implements a production style Raft consensus protocol in Go. It includes leader election, log replication, crash safe persistence, and snapshot based log compaction, and uses Porcupine to check linearizability.
It also implements a Raft backed replicated KV store. It also includes an observability layer which tracks metrics for the Raft and KV store. I have summarized my learnings in the blog post [Agreeing under Chaos](https://vaishdho1.github.io/my-portfolio/raft-consensus-protocol.html).

## Features

- **Leader Election**: Randomized timeouts
- **Log Replication**: Optimized replication with fast conflict resolution
- **Persistence**: Crash safe state and snapshot persistence with atomic writes
- **Snapshots**: Local log compaction with InstallSnapshot RPC for lagging followers
- **Fault Tolerance**: Handles network partitions, node crashes, and message loss/reordering
- **Strong Consistency**: Linearizable operations via state machine replication 
- **Observability**: Metrics hooks for election, replication, persistence, and apply latency

## Architecture

### Raft Consensus Module (`raft1/`)

The core Raft implementation provides:
- **Leader Election**: Heartbeats, RequestVote RPC, randomized election timeouts
- **Log Replication**: AppendEntries RPC with fast conflict resolution (XTerm/XIndex/XLen)
- **Replication Engine**: Per-peer replicator loops with trigger channels for immediate log pushes
- **Persistence**: Disk persistence with both synchronous and asynchronous persistence loops
- **Snapshots**: `Snapshot()` and `InstallSnapshot` RPC for log compaction
- **State Machine Interface**: Commits log entries via `ApplyMsg` channel
- **Observability**: Metrics sink integration for tracing Raft activity

### Replicated Key/Value Store (`kvraft1/`)

The KV service is built on top of Raft using state machine replication:
- **Client/Server**: `src/kvraft1/client.go` and `src/kvraft1/server.go`
- **Apply path**: Raft delivers committed commands and installed snapshots to the service via the
  Raft `ApplyMsg` channel, the KV layer applies them deterministically.
- **Snapshots**: The KV layer can snapshot its state. Raft persists the snapshot and uses it
  to compact the log and to bring lagging peers up to date via `InstallSnapshot`.

## Implementation Highlights

### Fast Log Repair

Implement optimization from the extended Raft paper, allowing leaders to back off `nextIndex` by entire terms instead of one entry at a time.

### Replication Engine

- Per-peer replication loops wake on heartbeats or explicit triggers
- RPC calls are bounded with a timeout to avoid stalled replication

### Observability Metrics

- Pluggable metrics sink tracks elections, replication, persistence, and apply path latency
- Prometheus backed sink is available for dashboards and alerting

### Snapshot-Based Compaction

- Log entries are periodically compacted into snapshots
- Followers that fall behind receive snapshots via `InstallSnapshot` RPC
- The log is bounded, preventing unbounded growth

### Crash Safety

Persistent state (term, vote, log) and service snapshots are written through a `Persister`. Snapshot payloads include (index,term) metadata to support safe recovery and restart reconciliation.

### Persistence and Snapshotting
- There are two persistence modes: Disk mode and Memory mode.
Memory mode is the default mode. Disk mode is enabled by setting the `PERSISTER_DISK` environment variable to `1`.

#### Features:

- **Decoupled I/O from the Raft**: Persistence requests are queued and handled by a background loop so Raft can continue processing RPCs without blocking on disk.
- **Synchronous persistence**: Some transitions (e.g: term/vote changes and durable snapshot installation) use synchronous persistence to ensure correctness across crashes.
- **Snapshot writes**: Local snapshots are persisted asynchronously.
 `InstallSnapshot` from the leader persists the received snapshot synchronously before queuing it to the state machine.

### KV Store Semantics (on top of Raft)

- **Versioned writes**: `Put(key, value, expectedVersion)` provides optimistic concurrency control per key.
- **Client uncertainty**: The client may return `ErrMaybe` when it cannot safely determine whether a `Put` applied during retries/leader churn.
- **Snapshot driven compaction**: The KV layer snapshots state and Raft uses it to compact the log and
  catch up lagging replicas via `InstallSnapshot`.

> **Note**: This implementation is based on MIT 6.5840 (formerly 6.824) Distributed Systems course materials. 
> 
> **My Work**: The entire core Raft implementation (`raft1/raft.go`). KV store implementation (`kvraft1/server.go`), (`kvraft1/client.go`), and (`kvraft1/rsm.go`).
> 
> **Course-Provided**:Memory mode tester (`tester1/`), RPC framework (`labrpc/`), serialization library (`labgob/`), and test files. These are included for completeness but are not my original work.

## References

- [Raft Paper](https://raft.github.io/raft.pdf)





