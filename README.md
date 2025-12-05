# Raft Consensus Implementation in Go

A production-style implementation of the Raft consensus algorithm in Go. This project implements the complete Raft protocol including leader election, log replication, crash safe persistence, and snapshot based log compaction. The Raft module can be used as a foundation for building distributed systems like replicated key-value stores, message queues, or other state machine applications.

> **Note**: This implementation is based on MIT 6.5840 (formerly 6.824) Distributed Systems course materials. 
> 
> **My Work**: The core Raft implementation (`raft1/raft.go`) - all logic for leader election, log replication, persistence, and snapshots.
> 
> **Course-Provided**: Test infrastructure (`tester1/`), RPC framework (`labrpc/`), serialization library (`labgob/`), and test files. These are included for completeness but are not my original work.

## Features

- ✅ **Leader Election**: Randomized timeouts
- ✅ **Log Replication**: Fast nextIndex backoff using XTerm/XIndex/XLen optimization
- ✅ **Persistence**: Crash safe state persistence with atomic writes
- ✅ **Snapshots**: Log compaction with InstallSnapshot RPC for lagging followers
- ✅ **Fault Tolerance**: Handles network partitions, node crashes, and message loss/reordering
- ✅ **Strong Consistency**: Linearizable operations via state machine replication

## Architecture

### Raft Consensus Module (`raft1/`)

The core Raft implementation provides:
- **Leader Election**: Heartbeats, RequestVote RPC, randomized election timeouts
- **Log Replication**: AppendEntries RPC with fast conflict resolution (XTerm/XIndex/XLen)
- **Persistence**: `persist()` and `readPersist()` for crash recovery
- **Snapshots**: `Snapshot()` and `InstallSnapshot` RPC for log compaction
- **State Machine Interface**: Commits log entries via `ApplyMsg` channel

## Prerequisites

- Go 1.22 or later
- Git

## Installation

```bash
git clone <your-repo-url>
cd raftkv/src
go mod download
```

## Building

```bash
cd src
go build ./raft1
```


## Implementation Highlights

### Fast Log Repair

Implements the XTerm/XIndex/XLen optimization from the extended Raft paper, allowing leaders to back off `nextIndex` by entire terms instead of one entry at a time.

### Snapshot-Based Compaction

- Log entries are periodically compacted into snapshots
- Followers that fall behind receive snapshots via `InstallSnapshot` RPC
- In-memory log is bounded, preventing unbounded growth

### Crash Safety

All persistent state (term, vote, log, snapshot metadata) is atomically persisted before any state changes, ensuring correct recovery after crashes.

## References

- [Raft Paper](https://raft.github.io/raft.pdf)
- [Extended Raft Paper](https://github.com/ongardie/dissertation/blob/master/stanford.pdf)
- [MIT 6.5840 (formerly 6.824)](https://pdos.csail.mit.edu/6.824/)

## License

This project is based on MIT 6.5840 course materials. Please refer to the course license for usage terms.

## Future Work

Potential applications to build on top of this Raft implementation:
- **Distributed Message Queue**: Replicated message broker with at-least-once delivery guarantees
- **Distributed Configuration Store**: Consistent configuration management across clusters
- **Distributed Lock Service**: Distributed locking primitives
- **Custom State Machines**: Any application requiring strong consistency and fault tolerance

