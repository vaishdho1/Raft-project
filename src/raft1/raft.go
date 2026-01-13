package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"raftkv/labgob"
	"raftkv/labgob"
	"raftkv/labrpc"
	"raftkv/raftapi"
	tester "raftkv/tester1"
)

var raftDebug = os.Getenv("RAFT_DEBUG") == "1"

func raftDPrintf(format string, args ...any) {
	if raftDebug {
		log.Printf("[raft] "+format, args...)
	}
}

// Consants for leader election
const (
	HeartbeatInterval  = 100 * time.Millisecond
	MinElectionTimeout = 300
	MaxElectionTimeout = 600
)

type RaftState int

const (
	StateFollower RaftState = iota
	StateCandidate
	StateLeader
)

// A Go object implementing a single Raft peer.
type LogEntry struct {
	Cmd  interface{}
	Term int
}

type Raft struct {
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *tester.Persister   // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	state       RaftState           // The current state of the server
	applyCh     chan raftapi.ApplyMsg
	applyMu     sync.Mutex // serialize applyCh send vs close
	applyClosed bool
	commitCond  sync.Cond
	CurrentTerm int        //Latest term server has seen
	VotedFor    int        //Peer that received vote in the currentTerm (initially -1)
	Log         []LogEntry // Create a log of LogEntry

	commitIndex int32 //Index of the highest log entry known to be committed (Internal)
	lastApplied int32 //Index of highest log entry applied to state machine (External)
	//Leader related configs
	nextIndex     []int //The next index that the leader assumes that needs to be sent
	matchIndex    []int //The highest log entry known to be replicated on each servers
	LastEntryTerm []int //Leader's last index for each term used to move back by more than 1 step in case of conflicts
	//Timeouts for hearbeats and elections
	electiontimeout time.Duration
	HeartbeatTime   time.Time
	// Snapshot related configurations
	//If this is set, the committer needs to first send the snapshot followed by the log
	pendingSnapshot *raftapi.ApplyMsg
	StartIndex      int
	snapshot        []byte // The snapshot computed so far
	//Persistence triggering
	persistenceTriggerch chan struct{}  // Asynchronous blocking channel
	persistWaitch        chan chan bool //Synchronous blocking with done
	persistState         *LogPersist
	//This is just for persist state
	persistLock sync.Mutex
	//Shutdown related variables
	shutdownCh chan struct{} // Set this channel to stop all persistence loops
}

// sendApplyMsg attempts to deliver an ApplyMsg to the service.

func (rf *Raft) sendApplyMsg(msg raftapi.ApplyMsg) bool {
	// Fast-path: if already closed, don't send.
	rf.applyMu.Lock()
	if rf.applyClosed {
		rf.applyMu.Unlock()
		return false
	}
	ch := rf.applyCh
	rf.applyMu.Unlock()

	// If applyCh is closed concurrently after we drop applyMu, a send would panic.
	// That's fine during shutdown; treat it as "not delivered".
	defer func() {
		_ = recover()
	}()

	select {
	case ch <- msg:
		return true
	case <-rf.shutdownCh:
		return false
	}
}

type LogPersist struct {
	VotedFor    int
	CurrentTerm int
	StartIndex  int
	LogCopy     []LogEntry
	//I am also keeping the snapshot here
	Snapshot []byte
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.CurrentTerm
	// After Kill(), higher layers (e.g., RSM.Submit polling GetState) must not
	// see this peer as leader, or they can spin until a timeout.
	isleader = rf.state == StateLeader && !rf.killed()

	return term, isleader
}
func (rf *Raft) persistenceLoop() {
	for !rf.killed() {
		select {
		case <-rf.shutdownCh:
			return
		case <-rf.persistenceTriggerch:
			rf.savePendingState()
		case doneCh := <-rf.persistWaitch:
			//Notify the waiter
			rf.savePendingState()
			close(doneCh)
		}
	}
}

func (rf *Raft) savePendingState() {
	if rf.killed() {
		return
	}
	rf.persistLock.Lock()
	state := rf.persistState
	rf.persistLock.Unlock()
	if state == nil {
		return
	}
	w := new(bytes.Buffer)
	encoder := labgob.NewEncoder(w)
	encoder.Encode(state.CurrentTerm)
	encoder.Encode(state.VotedFor)
	encoder.Encode(state.StartIndex) // where the log starts from
	encoder.Encode(state.LogCopy)
	raftstate := w.Bytes()
	// Persist raftstate only; snapshot is persisted independently.
	//TODO: If this fails the server should stop. Need mechanism where disk failures cause server to stop accepting requests and restart
	if rf.killed() {
		return
	}
	err := rf.persister.SaveRaftState(raftstate)
	if err != nil {
		//This is harmless, this means the particular instance cannot write anymore
		if errors.Is(err, tester.ErrFrozen) {
			return
		}
		//Any other failures, need to end the raft instance not trustable anymore
		rf.Kill()
		return

	}
	//After saving the state we want to update the matchInd for this peer
	rf.mu.Lock()

	lastInd := state.StartIndex + len(state.LogCopy) - 1
	// If this persisted state is stale relative to our current snapshot window,
	// ignore it. Otherwise triggerCommitter() can compute negative indices.
	if lastInd < rf.StartIndex {
		rf.mu.Unlock()
		return
	}
	if rf.matchIndex[rf.me] < lastInd {
		rf.matchIndex[rf.me] = lastInd
		//Check if there are new values to commit
		//rf.mu.Unlock()
		rf.triggerCommitter()
		//return
	}
	rf.mu.Unlock()

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var startIndex int
	var logEntries []LogEntry
	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&startIndex) != nil ||
		decoder.Decode(&logEntries) != nil {
		return
	}

	snapPayload, snapIndex, snapTerm := rf.persister.ReadSnapshotMeta()

	rf.mu.Lock()

	rf.CurrentTerm = currentTerm
	rf.VotedFor = votedFor
	rf.StartIndex = startIndex         // Load the start index from raftstate
	rf.commitIndex = int32(startIndex) // Start with this as committed
	rf.lastApplied = int32(startIndex)
	rf.Log = logEntries
	rf.snapshot = snapPayload

	// If snapshot metadata is ahead of raftstate StartIndex, we trust the snapshot
	// for the prefix <= snapIndex and reconcile the in memory log accordingly.
	if snapIndex > rf.StartIndex {
		sliceIndex := snapIndex - rf.StartIndex
		fileLog := rf.Log
		if sliceIndex >= 0 && sliceIndex < len(fileLog) {
			// Keep everything AFTER snapIndex.
			rf.Log = []LogEntry{{Cmd: nil, Term: snapTerm}}
			rf.Log = append(rf.Log, fileLog[sliceIndex+1:]...)
			rf.StartIndex = snapIndex
		} else if sliceIndex >= len(fileLog) {
			// Snapshot supersedes the whole log.
			rf.Log = []LogEntry{{Cmd: nil, Term: snapTerm}}
			rf.StartIndex = snapIndex
		} else {
			// sliceIndex < 0 => snapshot is older than raftstate; ignore snapshot meta.
		}
		rf.commitIndex = max(rf.commitIndex, int32(rf.StartIndex))
		rf.lastApplied = max(rf.lastApplied, int32(rf.StartIndex))
		//Make sure we persist the new log here immediately
		logCopy := make([]LogEntry, len(rf.Log))
		copy(logCopy, rf.Log)
		w := new(bytes.Buffer)
		encoder := labgob.NewEncoder(w)
		encoder.Encode(rf.CurrentTerm)
		encoder.Encode(rf.VotedFor)
		encoder.Encode(rf.StartIndex)
		encoder.Encode(logCopy)
		raftstate := w.Bytes()
		rf.mu.Unlock()
		// Direct write to persister
		err := rf.persister.SaveRaftState(raftstate)
		if err != nil {
			//This is harmless, this means the particular instance cannot write anymore
			if errors.Is(err, tester.ErrFrozen) {
				return
			}
			//Any other failures, need to end the raft instance not trustable anymore
			rf.Kill()
			log.Printf("Panic: failed to save raftstate %v", err)
			return

		}
		return
	}

	rf.mu.Unlock()

}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {

	return rf.persister.RaftStateSize()
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()

	if index <= rf.StartIndex {
		rf.mu.Unlock()
		return
	}
	if index <= rf.StartIndex || index > int(rf.commitIndex) || index > int(rf.lastApplied) {
		rf.mu.Unlock()
		return
	}
	raftDPrintf("me=%d Snapshot(index=%d): before StartIndex=%d commit=%d applied=%d loglen=%d snapBytes=%d",
		rf.me, index, rf.StartIndex, rf.commitIndex, rf.lastApplied, len(rf.Log), len(snapshot))
	sliceIndex := index - rf.StartIndex
	curStartIndex := rf.StartIndex
	term := rf.Log[sliceIndex].Term
	snapCopy := make([]byte, len(snapshot))
	copy(snapCopy, snapshot)
	rf.mu.Unlock()

	//Save the snapshot asynchronously
	ioDone := rf.persister.SaveSnapshotAsync(snapCopy, index, term)

	//After snapshot is durable, trim log, publish snapshot in memory, then trigger a raftstate persist
	go func(snapIndex int, snapTerm int, snapBytes []byte) {
		err := <-ioDone
		if err != nil {
			//These are harmless errors we can just return
			if errors.Is(err, tester.ErrFrozen) || errors.Is(err, tester.ErrSnapshotOutdated) {
				return
			}
			//Panic case: Something wrong with disk writes
			log.Printf("Critical Disk Failure: %v", err)
			rf.Kill()
			return
		}
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		// If we've already compacted past this point or the start index changed, this happens if
		// an installSnapshot() came after this.
		if snapIndex <= rf.StartIndex || rf.StartIndex != curStartIndex {
			rf.mu.Unlock()
			return
		}
		sliceIndex := snapIndex - rf.StartIndex
		if sliceIndex < len(rf.Log) {
			oldLog := rf.Log
			rf.Log = []LogEntry{{Cmd: nil, Term: snapTerm}}
			rf.Log = append(rf.Log, oldLog[sliceIndex+1:]...)
		} else {
			rf.Log = []LogEntry{{Cmd: nil, Term: snapTerm}}
		}
		rf.StartIndex = snapIndex
		rf.snapshot = snapBytes
		rf.commitIndex = max(rf.commitIndex, int32(rf.StartIndex))
		rf.lastApplied = max(rf.lastApplied, int32(rf.StartIndex))
		for i := range rf.nextIndex {
			if rf.nextIndex[i] <= rf.StartIndex {
				rf.nextIndex[i] = rf.StartIndex + 1
			}
		}
		rf.createStatePersistenceArgs()
		//Keep this asynchronous so that raft persistence always does the most latest write
		rf.sendTrigger()
		raftDPrintf("me=%d Snapshot(index=%d): after StartIndex=%d commit=%d applied=%d loglen=%d",
			rf.me, snapIndex, rf.StartIndex, rf.commitIndex, rf.lastApplied, len(rf.Log))
		rf.mu.Unlock()
	}(index, term, snapCopy)
}

type SnapshotRequestArgs struct {
	Term              int // Leader's term
	LeaderId          int
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte //raw bytes of the snapshot chunk, starting at
}
type SnapshotReplyArgs struct {
	Term int // Term for the leader to update
}

func (rf *Raft) InstallSnapshot(request_args *SnapshotRequestArgs, reply_args *SnapshotReplyArgs) {
	rf.mu.Lock()

	//First check if the term is smaller than the current term
	if request_args.Term < rf.CurrentTerm {
		reply_args.Term = rf.CurrentTerm
		rf.mu.Unlock()
		return
	}
	raftDPrintf("me=%d InstallSnapshot(from leader=%d term=%d idx=%d): before term=%d state=%d StartIndex=%d commit=%d applied=%d loglen=%d snapBytes=%d",
		rf.me, request_args.LeaderId, request_args.Term, request_args.LastIncludedIndex,
		rf.CurrentTerm, rf.state, rf.StartIndex, rf.commitIndex, rf.lastApplied, len(rf.Log), len(request_args.Data))
	//Check if this is a new term
	if request_args.Term > rf.CurrentTerm {
		rf.CurrentTerm = request_args.Term
		rf.VotedFor = -1
	}
	//Always become a follower if its the same or greater term
	rf.state = StateFollower
	rf.HeartbeatTime = time.Now()
	reply_args.Term = rf.CurrentTerm
	//If this is a stale snapshot, I have the most upto date log
	if request_args.LastIncludedIndex <= rf.StartIndex {
		raftDPrintf("me=%d InstallSnapshot(idx=%d): stale (StartIndex=%d) - ignore", rf.me, request_args.LastIncludedIndex, rf.StartIndex)
		rf.mu.Unlock()
		return
	}
	// Also stale if we've already applied past this point. Installing it would roll back the service state machine
	if request_args.LastIncludedIndex <= int(rf.lastApplied) {
		raftDPrintf("me=%d InstallSnapshot(idx=%d): stale (lastApplied=%d StartIndex=%d) - ignore",
			rf.me, request_args.LastIncludedIndex, rf.lastApplied, rf.StartIndex)
		rf.mu.Unlock()
		return
	}
	// Copy bytes so we don't retain an alias to the RPC buffer.
	snapCopy := make([]byte, len(request_args.Data))
	copy(snapCopy, request_args.Data)

	// Build ApplyMsg now (uses snapCopy, which is immutable).
	applymessage := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      snapCopy,
		SnapshotTerm:  request_args.LastIncludedTerm,
		SnapshotIndex: request_args.LastIncludedIndex,
	}

	// Persist snapshot first
	rf.mu.Unlock()
	err := rf.persister.SaveSnapshotSync(snapCopy, request_args.LastIncludedIndex, request_args.LastIncludedTerm)
	if err != nil {
		//These are harmless errors
		if errors.Is(err, tester.ErrSnapshotOutdated) || errors.Is(err, tester.ErrFrozen) {
			return
		}
		//This is harmful diskwrite failed so we need to kill this instance
		log.Fatalf("Critical Disk Failure: %v", err)
		rf.Kill()
		return
	}

	// Snapshot is durable. Now update in-memory state and persist the corresponding raftstate.
	rf.mu.Lock()
	// Re check staleness
	if request_args.Term < rf.CurrentTerm ||
		request_args.LastIncludedIndex <= rf.StartIndex ||
		request_args.LastIncludedIndex <= int(rf.lastApplied) {
		rf.mu.Unlock()
		return
	}

	// Now update the log and snapshot state.
	lastLogIndex := rf.StartIndex + len(rf.Log) - 1
	if request_args.LastIncludedIndex <= lastLogIndex {
		cut := request_args.LastIncludedIndex - rf.StartIndex
		// snapshot matches with a prefix so keep the suffix if term matches
		if rf.Log[cut].Term == request_args.LastIncludedTerm {
			rf.Log = rf.Log[cut:]
			rf.Log[0].Cmd = nil // dummy term
		} else {
			// Conflict at this point so erase everything.
			rf.Log = []LogEntry{{Cmd: nil, Term: request_args.LastIncludedTerm}}
		}
	} else {
		// This supersedes the entire log so need to completely replace.
		rf.Log = []LogEntry{{Cmd: nil, Term: request_args.LastIncludedTerm}}
	}

	rf.snapshot = snapCopy
	rf.StartIndex = request_args.LastIncludedIndex

	// Queue the snapshot barrier
	rf.pendingSnapshot = &applymessage
	rf.commitIndex = max(rf.commitIndex, int32(rf.StartIndex))
	rf.commitCond.Broadcast()

	// Persist the new raftstate
	rf.createStatePersistenceArgs()
	rf.mu.Unlock()
	rf.persistSynchronously()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	Votegranted bool
}

// Call this with rf.mu held .
// Note: The order of lock is rf.mu->presister and not the other way round
// If not locked in this order, it can lead to deadlock
func (rf *Raft) createStatePersistenceArgs() {
	logCopy := make([]LogEntry, len(rf.Log))
	copy(logCopy, rf.Log)
	rf.persistLock.Lock()
	rf.persistState = &LogPersist{
		VotedFor:    rf.VotedFor,
		CurrentTerm: rf.CurrentTerm,
		StartIndex:  rf.StartIndex,
		LogCopy:     logCopy,
		Snapshot:    rf.snapshot}
	rf.persistLock.Unlock()
}

// Call this with lock applied
func (rf *Raft) triggerCommitter() {
	size := len(rf.peers)
	lastIndex := rf.matchIndex[rf.me]
	// Never try to commit indices that are no longer in our log window
	if lastIndex < rf.StartIndex {
		lastIndex = rf.StartIndex
		rf.matchIndex[rf.me] = lastIndex
	}
	for newcommitInd := lastIndex; newcommitInd > int(rf.commitIndex); newcommitInd-- {
		// Nothing <= StartIndex can be committed via the log
		if newcommitInd <= rf.StartIndex {
			break
		}
		count := 0
		for r := range rf.matchIndex {
			if rf.matchIndex[r] >= newcommitInd {
				count += 1
			}
		}
		//Check if this is valid commitIndex and belongs to the current term
		index := newcommitInd - rf.StartIndex
		if index < 0 {
			panic(fmt.Sprintf("The commitIndex was not correctly updated commitindex:%d startindex:%d", newcommitInd, rf.StartIndex))
		}
		if count > size/2 && rf.Log[index].Term == rf.CurrentTerm {
			rf.commitIndex = int32(newcommitInd)
			//Broadcast everytime the commitIndex changes
			rf.commitCond.Broadcast()
			break
		}
	}

}

// Call this without any lock
func (rf *Raft) persistSynchronously() {
	if rf.killed() {
		return
	}
	doneCh := make(chan bool)
	// Enqueue the request, but abort if shutting down
	select {
	case rf.persistWaitch <- doneCh:
	case <-rf.shutdownCh:
		return
	}
	// Wait for completion, but abort if shutting down
	select {
	case <-doneCh:
		return
	case <-rf.shutdownCh:
		return
	}

}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	// If this Raft instance was killed, ignore the RPC.
	if rf.killed() {
		rf.mu.Unlock()
		return
	}
	//If the terms is lesser
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Votegranted = false
		rf.mu.Unlock()
		return
	}

	termChanged := false
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.state = StateFollower
		rf.VotedFor = -1
		termChanged = true
	}
	//Check if vote can be granted
	myLastIndex := len(rf.Log) - 1
	myLastTerm := rf.Log[myLastIndex].Term
	voteGranted := false
	upToDate := args.LastLogTerm > myLastTerm || (args.LastLogTerm == myLastTerm && args.LastLogIndex >= rf.StartIndex+myLastIndex)
	if upToDate && (rf.VotedFor == -1 || rf.VotedFor == args.CandidateId) {
		rf.state = StateFollower
		rf.VotedFor = args.CandidateId
		rf.HeartbeatTime = time.Now()
		voteGranted = true
	}
	reply.Votegranted = voteGranted
	reply.Term = rf.CurrentTerm
	if termChanged || voteGranted {
		//Create and store updated persistence state
		rf.createStatePersistenceArgs()
		rf.mu.Unlock()
		rf.persistSynchronously()
		return
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendTrigger() {

	select {
	//Try sending a message through the channel
	case rf.persistenceTriggerch <- struct{}{}:
		//If the thread was busy just skip as there is already a trigger
	default:
	}

}
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// If this Raft instance has been killed, report not leader.
	if rf.killed() {
		return -1, -1, false
	}
	//Check if I am the leader here
	if rf.state != StateLeader {
		return -1, -1, false
	}
	newLogEntry := LogEntry{Cmd: command, Term: rf.CurrentTerm}

	rf.Log = append(rf.Log, newLogEntry)
	index = rf.StartIndex + len(rf.Log) - 1
	term = rf.CurrentTerm
	rf.createStatePersistenceArgs()
	rf.sendTrigger()
	//Send appendRPC to the followers
	go rf.sendAppendRPC()

	return index, term, isLeader
}
func (rf *Raft) shutdownSystem() {
	rf.mu.Lock()
	select {
	case <-rf.shutdownCh:
		//Already closed nothing to do
	default:
		close(rf.shutdownCh)
	}
	// After shutdown, this peer should never behave like a leader again.
	// This is shutting down
	rf.state = StateFollower
	//Unblock commitCond channel
	rf.commitCond.Broadcast()
	//Close all channels which are waiting for a trigger
	rf.sendTrigger()
	rf.mu.Unlock()
	// Close applyCh so the service/RSM can stop its loops
	rf.applyMu.Lock()
	if !rf.applyClosed {
		close(rf.applyCh)
		rf.applyClosed = true
	}
	rf.applyMu.Unlock()

}
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Close all the channels
	rf.shutdownSystem()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

type AppendEntriesReqArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // Entries to send
	LeaderCommit int32      // Last index comitted on the leader
}
type AppendEntriesResponseArgs struct {
	Term    int //Current term for leader to update itself
	Success bool
	//If not a success end more information
	XTerm  int //term in the conflicting entry (if any)
	XIndex int //index of first entry with that term (if any)
	XLen   int //log length

}

func (rf *Raft) AppendEntries(args *AppendEntriesReqArgs, reply *AppendEntriesResponseArgs) {
	rf.mu.Lock()
	if rf.killed() {
		rf.mu.Unlock()
		return
	}
	//Check if its a valid leader or not
	if args.Term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	// Heartbeat from a valid leader (term >= ours) resets election timer.
	rf.HeartbeatTime = time.Now()
	// If term is higher, update term and clear vote. If term is equal, we should still step down to follower
	termChanged := false
	if args.Term > rf.CurrentTerm {
		rf.CurrentTerm = args.Term
		rf.VotedFor = -1
		termChanged = true
		//rf.persist()
	}
	//Always become a follower if its the same or greater term
	rf.state = StateFollower
	reply.Term = rf.CurrentTerm
	//Ignore this since you are sending an index smaller than my startIndex, this is definetely stale.
	if args.PrevLogIndex < rf.StartIndex || rf.StartIndex+len(rf.Log)-1 < args.PrevLogIndex {
		reply.Success = false
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = rf.StartIndex + len(rf.Log)
		if args.PrevLogIndex < rf.StartIndex {
			reply.XIndex = rf.StartIndex + 1
		}
		//Before returning persist
		if termChanged {
			rf.createStatePersistenceArgs()
			rf.mu.Unlock()
			rf.persistSynchronously()
			return
		}
		rf.mu.Unlock()
		return
	}

	if rf.Log[args.PrevLogIndex-rf.StartIndex].Term != args.PrevLogTerm {
		conflictTerm := rf.Log[args.PrevLogIndex-rf.StartIndex].Term
		i := args.PrevLogIndex - rf.StartIndex
		for i > 0 && rf.Log[i].Term == conflictTerm {
			i--
		}
		//Send back the first index this term was seen
		reply.XIndex = rf.StartIndex + i + 1 //Sending the first entry for this term in the log
		reply.XTerm = conflictTerm
		reply.XLen = rf.StartIndex + len(rf.Log) //Send the updated length for the nextIndex
		reply.Success = false
		// If we updated term/vote above, persist before replying (even on failure).
		if termChanged {
			rf.createStatePersistenceArgs()
			rf.mu.Unlock()
			rf.persistSynchronously()
			return
		}
		rf.mu.Unlock()
		return
	}
	//Find out the first conflicting entry and append everything after it
	insertIndex := args.PrevLogIndex + 1
	logChanged := false
	for _, entry := range args.Entries {
		if insertIndex >= rf.StartIndex+len(rf.Log) {
			rf.Log = append(rf.Log, entry)
			logChanged = true
		} else if rf.Log[insertIndex-rf.StartIndex].Term != entry.Term {
			rf.Log = rf.Log[:insertIndex-rf.StartIndex]
			rf.Log = append(rf.Log, entry)
			logChanged = true
		}
		insertIndex++
	}
	//If my log changed make sure we persist
	if logChanged || termChanged {
		rf.createStatePersistenceArgs()
		rf.mu.Unlock()
		rf.persistSynchronously()
		//We take the lock and first check if the term has changed
		rf.mu.Lock()
	}
	//Need to check killed to exit
	if rf.killed() {
		rf.mu.Unlock()
		return
	}
	if rf.CurrentTerm != args.Term {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	//Update the commitIndex to the latest value to commit if the old value was not the same. This will
	//prevent waking the committer routine unnecessarily
	if args.LeaderCommit > rf.commitIndex {
		lastVerifiedIndex := args.PrevLogIndex + len(args.Entries)
		newCommit := min(args.LeaderCommit, int32(lastVerifiedIndex))
		//Make sure the commit index doesnt go backward
		if newCommit > int32(rf.StartIndex) && newCommit > rf.commitIndex {
			rf.commitIndex = newCommit
			rf.commitCond.Broadcast()
		}
	}
	reply.Success = true
	rf.mu.Unlock()

}
func (rf *Raft) SendInstallSnapshot(server int, args *SnapshotRequestArgs, reply *SnapshotReplyArgs) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// This is called with rf.mu held
func (rf *Raft) sendSnapshotTo(server int) bool {

	snapCopy := make([]byte, len(rf.snapshot))
	copy(snapCopy, rf.snapshot)
	snapshotRequestArgs := &SnapshotRequestArgs{
		Term:              rf.CurrentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.StartIndex,
		LastIncludedTerm:  rf.Log[0].Term,
		Data:              snapCopy,
	}
	snapshotReplyargs := &SnapshotReplyArgs{}
	rf.mu.Unlock()
	ok := rf.SendInstallSnapshot(server, snapshotRequestArgs, snapshotReplyargs)
	rf.mu.Lock()
	if !ok {
		return false
	}
	//Dont send if I am killed
	if rf.killed() {
		return false
	}
	//If its an old snapshot reply, ignore
	if snapshotRequestArgs.Term < rf.CurrentTerm {
		return false
	}
	// QUORUM_CHECK: we heard back from this peer in our current term.

	// If follower reports a higher term, step down and clear vote for the new term.
	if snapshotReplyargs.Term > rf.CurrentTerm {
		rf.state = StateFollower
		rf.VotedFor = -1
		rf.CurrentTerm = snapshotReplyargs.Term
		rf.createStatePersistenceArgs()
		rf.mu.Unlock()
		rf.persistSynchronously()
		rf.mu.Lock()
		return false
	}

	if rf.state != StateLeader {
		return false
	}

	rf.nextIndex[server] = rf.StartIndex + 1
	return true
}
func (rf *Raft) sendAppendRPC() {

	// If we've been killed, don't start any new work.
	if rf.killed() {
		return
	}
	// Make sure you create the requestArgs here and send
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			//Create requestargs for sending heartbeat
			//Call the append rpc for this server
			rf.mu.Lock()
			//QOURUM_CHECK:Check if I am still the leader and not killed
			if rf.killed() || rf.state != StateLeader {
				rf.mu.Unlock()
				return
			}
			//This holds the snapshot updated index
			nxtInd := rf.nextIndex[server]
			prevInd := nxtInd - 1
			request_args := &AppendEntriesReqArgs{
				Term:         rf.CurrentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevInd,
				PrevLogTerm:  rf.Log[prevInd-rf.StartIndex].Term,
				LeaderCommit: rf.commitIndex,
			}

			//Create entries depending on what should be sent
			var entries []LogEntry
			lastInd := len(rf.Log) - 1
			//I need to send the fresh entries
			if prevInd-rf.StartIndex+1 <= lastInd {
				entries = rf.Log[prevInd-rf.StartIndex+1:]
			} else {
				entries = nil
			}

			entriesCopy := make([]LogEntry, len(entries))
			copy(entriesCopy, entries)
			request_args.Entries = entriesCopy
			rf.mu.Unlock()
			reply_args := &AppendEntriesResponseArgs{}
			ok := rf.peers[server].Call("Raft.AppendEntries", request_args, reply_args)
			//If Append entries was not successful return
			if !ok {
				return
			}
			rf.mu.Lock()
			//Return if you are killed
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
			//Check for stale response, if this not from the same term as my current term cannot accept it
			if rf.state != StateLeader || request_args.Term != rf.CurrentTerm || rf.nextIndex[server]-1 != request_args.PrevLogIndex {
				rf.mu.Unlock()
				return
			}
			if reply_args.Term > rf.CurrentTerm {
				rf.state = StateFollower
				rf.VotedFor = -1
				rf.CurrentTerm = reply_args.Term
				rf.createStatePersistenceArgs()
				rf.mu.Unlock()
				rf.persistSynchronously()
				return
			}
			//If not I need to update either my state or the state of the server
			if !reply_args.Success {

				if rf.nextIndex[server] == rf.StartIndex+1 {
					//Send snapshot to the server
					if !rf.sendSnapshotTo(server) {
						rf.mu.Unlock()
						return
					}
				}
				//The log is too short, send the entry at the last index of the log
				if reply_args.XIndex == -1 {
					if reply_args.XLen <= rf.StartIndex {
						if !rf.sendSnapshotTo(server) {
							rf.mu.Unlock()
							return
						}
					} else {
						//Make sure nextInd is not larger thant the log size
						rf.nextIndex[server] = min(reply_args.XLen, rf.StartIndex+len(rf.Log))
					}
				} else {
					lastIndForX := -1
					for i := len(rf.Log) - 1; i >= 1; i-- {
						if rf.Log[i].Term == reply_args.XTerm {
							lastIndForX = i
							break
						} //Break if the term is not present
						if rf.Log[i].Term < reply_args.XTerm {
							break
						}
					}
					//The term is not present, send whatever is present at the index the reply sent
					if lastIndForX == -1 {
						//Check if it still the part of the log
						if reply_args.XIndex <= rf.StartIndex {
							if !rf.sendSnapshotTo(server) {
								rf.mu.Unlock()
								return
							}
						} else {
							//Make sure nextInd is not larger thant the log size
							rf.nextIndex[server] = min(reply_args.XIndex, rf.StartIndex+len(rf.Log))
						}
					} else {
						rf.nextIndex[server] = rf.StartIndex + lastIndForX + 1 //Update the new index
					}

				}

			} else {
				newMatchIndex := request_args.PrevLogIndex + len(request_args.Entries)
				rf.matchIndex[server] = newMatchIndex
				rf.nextIndex[server] = newMatchIndex + 1
				rf.triggerCommitter()
			}
			//Unlock here before exiting
			rf.mu.Unlock()

		}(i)
	}

}

func (rf *Raft) start_heartbeats() {
	//Send append entry heartbeats every few seconds
	for !rf.killed() {
		rf.mu.Lock()

		//Start a go routine to sendAppendRPCs
		if !rf.killed() && rf.state == StateLeader {
			go rf.sendAppendRPC()
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
			return
		}
		//Sleep for HeartbeatInterval
		time.Sleep(HeartbeatInterval)
	}
}

// Creates a new set of args every election and sends it over
func (rf *Raft) create_state() *RequestVoteArgs {
	rf.mu.Lock()
	//Increment the term and start an election
	rf.state = StateCandidate
	rf.CurrentTerm += 1
	rf.VotedFor = rf.me
	//Persist after starting the election: synchronously
	rf.createStatePersistenceArgs()
	rf.mu.Unlock()
	rf.persistSynchronously()
	rf.mu.Lock()

	//Take the lock again and check if its still a candidate
	if rf.killed() {
		rf.mu.Unlock()
		return nil
	}
	if rf.state != StateCandidate {
		rf.mu.Unlock()
		return nil
	}
	lastIndex := len(rf.Log) - 1
	requestargs := &RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.StartIndex + lastIndex, //Need to point this to the actual index
		LastLogTerm:  rf.Log[lastIndex].Term}
	rf.mu.Unlock()
	return requestargs
}
func (rf *Raft) start_election() {

	//Create the args required
	requestargs := rf.create_state()
	//Abort election if I am not a candidate in the mean time or the server was killed
	if requestargs == nil {
		return
	}

	size := len(rf.peers)
	count := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			replyargs := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, requestargs, replyargs)
			if !ok {
				return
			}
			rf.mu.Lock()
			//Check if killed
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
			//If it is not a candidate anymore ignore everything here
			if rf.state != StateCandidate {
				rf.mu.Unlock()
				return
			}

			if !replyargs.Votegranted {
				if replyargs.Term > rf.CurrentTerm {
					rf.state = StateFollower
					rf.VotedFor = -1
					rf.CurrentTerm = replyargs.Term
					//Since I changed the term need to persist synchronously
					rf.createStatePersistenceArgs()
					rf.mu.Unlock()
					rf.persistSynchronously()
					return
				}
				rf.mu.Unlock()
				return
			}
			//If vote was granted but it was a stale vote
			if replyargs.Term < rf.CurrentTerm {
				rf.mu.Unlock()
				return
			}
			//Did you grant me a vote here
			if replyargs.Votegranted {
				count += 1
				//Check if I am still a candidate and won the election
				if rf.state == StateCandidate && count > size/2 {
					rf.state = StateLeader
					for peer := range rf.peers {
						// Index should reflect the shanpshot presence. Log starts from startIndex now everything before that is in a
						//snapshot
						rf.nextIndex[peer] = len(rf.Log) + rf.StartIndex
						rf.matchIndex[peer] = 0
					}
					//Start a heartbeat loop here
					go rf.start_heartbeats()
				}
			}
			rf.mu.Unlock()
		}(i)

	}
}
func (rf *Raft) committer() {
	//Run a loop which periodially checks in all changes
	for !rf.killed() {
		rf.mu.Lock()

		for !rf.killed() && rf.pendingSnapshot == nil &&
			(rf.lastApplied < int32(rf.StartIndex) || rf.lastApplied >= rf.commitIndex) {
			rf.commitCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		// Snapshot barrier: deliver snapshot before any later commands.
		if rf.pendingSnapshot != nil {
			snapshotMsg := *rf.pendingSnapshot
			rf.pendingSnapshot = nil
			rf.mu.Unlock()
			rf.sendApplyMsg(snapshotMsg)
			rf.mu.Lock()
			rf.commitIndex = max(rf.commitIndex, int32(snapshotMsg.SnapshotIndex))
			rf.lastApplied = max(rf.lastApplied, int32(snapshotMsg.SnapshotIndex))
			rf.mu.Unlock()
			continue
		}

		rf.lastApplied++
		if rf.lastApplied < int32(rf.StartIndex) {
			panic("Last applied is less than start index, " + strconv.Itoa(int(rf.StartIndex)) + " got: " + strconv.Itoa(int(rf.lastApplied)))
		}
		msg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.Log[rf.lastApplied-int32(rf.StartIndex)].Cmd,
			CommandIndex: int(rf.lastApplied),
		}
		rf.mu.Unlock()
		rf.sendApplyMsg(msg)

	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		rf.mu.Lock()
		//Check if the difference between the current time and prevous heartbeat has exceeded election timeout
		if rf.state != StateLeader && time.Since(rf.HeartbeatTime) > rf.electiontimeout {

			rf.HeartbeatTime = time.Now()
			//Update election timeout everytime you start an election so that no two servers can have the same value forever
			rf.electiontimeout = time.Duration(MinElectionTimeout+rand.Intn(MaxElectionTimeout-MinElectionTimeout)) * time.Millisecond
			//start an election
			go rf.start_election()
		}

		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {

	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = StateFollower //Starts as a follower intiially
	rf.applyCh = applyCh
	rf.shutdownCh = make(chan struct{})
	//Assign the commit condition to a specific lock
	rf.commitCond = *sync.NewCond(&rf.mu)
	rf.VotedFor = -1
	rf.CurrentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	//Removing this as the log will be empty initially
	rf.Log = append(rf.Log, LogEntry{Cmd: nil, Term: 0})
	rf.HeartbeatTime = time.Now()
	rf.electiontimeout = time.Duration(MinElectionTimeout+rand.Intn(MaxElectionTimeout-MinElectionTimeout)) * time.Millisecond
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.persistenceTriggerch = make(chan struct{}, 1)
	rf.persistWaitch = make(chan chan bool)
	rf.StartIndex = 0
	// Initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//Start the persistence loop
	go rf.persistenceLoop()
	// start ticker goroutine to start elections
	go rf.ticker()
	//Start committer to periodically add the committed values to the state machine
	go rf.committer()

	return rf
}
