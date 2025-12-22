package rsm

import (
	"log"
	"os"
	"sync"
	"time"

	"raftkv/kvsrv1/rpc"
	"raftkv/labrpc"
	raft "raftkv/raft1"
	"raftkv/raftapi"
	tester "raftkv/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

var rsmDebug = os.Getenv("RSM_DEBUG") == "1"

func rsmDPrintf(format string, args ...any) {
	if rsmDebug {
		log.Printf("[rsm] "+format, args...)
	}
}

// This is used to send the command
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  int
	Req any
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type OpResult struct {
	Res   any
	Cmd   any
	Index int
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	id      int
	waiters map[int]chan OpResult // waiters keyed by Raft log index
	applied map[int]OpResult      // applied results keyed by index (handles Submit/Reader race)
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		// Buffered to reduce coupling between Raft sender goroutines and the Reader.
		applyCh: make(chan raftapi.ApplyMsg, 1024),
		sm:      sm,
		id:      0,
		waiters: make(map[int]chan OpResult),
		applied: make(map[int]OpResult),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	//Restore snapshot on startup if one exists
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		rsmDPrintf("me=%d startup restore snapshot bytes=%d", me, len(snapshot))
		rsm.sm.Restore(snapshot)
	}
	go rsm.Reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) Reader() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			op, ok := msg.Command.(Op)
			if !ok {
				continue
			}
			//log.Printf("Applying request: %T", op.Req)
			res := rsm.sm.DoOp(op.Req)
			result := OpResult{Res: res, Cmd: op, Index: msg.CommandIndex}

			rsm.mu.Lock()
			ch, ok := rsm.waiters[msg.CommandIndex]
			if ok {
				delete(rsm.waiters, msg.CommandIndex)
				rsm.mu.Unlock()
				ch <- result

			} else {
				// No local Submit() is waiting (common on followers), or Submit() hasn't
				// registered its waiter yet (race). Only the originating server needs to
				// remember the result to avoid a lost wakeup.
				if op.Me == rsm.me {
					rsm.applied[msg.CommandIndex] = result
				}
				rsm.mu.Unlock()
			}
			// Snapshot when we're approaching maxraftstate (use 80% threshold to keep logs trimmed)
			if rsm.maxraftstate != -1 {
				threshold := rsm.maxraftstate * 8 / 10
				if rsm.rf.PersistBytes() >= threshold {
					snapshotbytes := rsm.sm.Snapshot()
					rsm.rf.Snapshot(msg.CommandIndex, snapshotbytes)
				}
			}
		}
		if msg.SnapshotValid {
			rsm.sm.Restore(msg.Snapshot)

			rsm.mu.Lock()
			for idx := range rsm.waiters {
				if idx <= msg.SnapshotIndex {
					close(rsm.waiters[idx])
					delete(rsm.waiters, idx)
				}
			}
			for idx := range rsm.applied {
				if idx <= msg.SnapshotIndex {
					delete(rsm.applied, idx)
				}
			}
			rsm.mu.Unlock()
		}
	}
	//Make sure you stop submit after shutdown
	rsm.mu.Lock()
	for _, ch := range rsm.waiters {
		close(ch)
	}
	rsm.waiters = make(map[int]chan OpResult)
	rsm.applied = make(map[int]OpResult)
	rsm.mu.Unlock()

}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	rsm.mu.Lock()
	rsm.id++
	id := rsm.id
	rsm.mu.Unlock()

	op := Op{Me: rsm.me, Id: id, Req: req}
	index, startTerm, isLeader := rsm.rf.Start(op)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	// Register waiter and handle the race where Reader applied this index before
	// we registered the waiter.
	rsm.mu.Lock()
	ch := make(chan OpResult, 1)
	rsm.waiters[index] = ch
	if res, ok := rsm.applied[index]; ok {
		delete(rsm.applied, index)
		delete(rsm.waiters, index)
		rsm.mu.Unlock()
		if res.Cmd.(Op) == op {
			return rpc.OK, res.Res
		}
		return rpc.ErrWrongLeader, nil
	}
	rsm.mu.Unlock()

	// Ensure waiter cleanup on all return paths.
	defer func() {
		rsm.mu.Lock()
		delete(rsm.waiters, index)
		rsm.mu.Unlock()
	}()

	for {
		select {
		case notif, ok := <-ch:
			if !ok {
				return rpc.ErrWrongLeader, nil
			}
			if notif.Cmd.(Op) == op {
				return rpc.OK, notif.Res
			}
			return rpc.ErrWrongLeader, nil
		case <-time.After(50 * time.Millisecond):
			// If we learn about a new term / lost leadership after healing,
			// the operation may have been discarded and will never commit.
			curTerm, stillLeader := rsm.rf.GetState()
			if !stillLeader || curTerm != startTerm {
				return rpc.ErrWrongLeader, nil
			}
		}
	}

}

// your code here
