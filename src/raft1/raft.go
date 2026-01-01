package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
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
	StartIndex int
	snapshot   []byte // The snapshot computed so far
	//Persistence triggering
	persistenceTriggerch chan struct{}        // Asynchronous blocking channel
	snapshotCh           chan SnapshotRequest //Synchronous blocking with done
	persistWaitch        chan chan bool       //Synchronous blocking with done
	persistState         *LogPersist
	//This is just for persist state
	persistLock sync.Mutex
	// In-memory length of the raft state
	raftStateSize int64
}

// This is used to send the snapshot arguements through the channel
type SnapshotRequest struct {
	Snapshot    []byte
	VotedFor    int
	CurrentTerm int
	StartIndex  int
	LogCopy     []LogEntry
	Done        chan bool
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
	isleader = rf.state == StateLeader

	return term, isleader
}
func (rf *Raft) persistenceLoop() {
	for {
		select {
		case <-rf.persistenceTriggerch:
			rf.savePendingState()
		case doneCh := <-rf.persistWaitch:
			rf.savePendingState()
			//Notify the waiter
			close(doneCh)
		//When persisting snapshot we persist the entire raft state too since that changes everytime there is a new snaphsot
		case req := <-rf.snapshotCh:
			w := new(bytes.Buffer)
			encoder := labgob.NewEncoder(w)
			encoder.Encode(req.CurrentTerm)
			encoder.Encode(req.VotedFor)
			encoder.Encode(req.StartIndex) // where the log starts from
			encoder.Encode(req.LogCopy)
			raftstate := w.Bytes()
			rf.persister.Save(raftstate, req.Snapshot)
			if req.Done == nil {
				panic("Done channel is nil during snapshotting")
			}
			close(req.Done)

		}
	}
}
func (rf *Raft) GetRaftStateSize() int {
	// This is super fast and thread-safe
	return int(atomic.LoadInt64(&rf.raftStateSize))
}
func (rf *Raft) savePendingState() {
	rf.persistLock.Lock()
	state := rf.persistState
	rf.persistLock.Unlock()
	w := new(bytes.Buffer)
	encoder := labgob.NewEncoder(w)
	encoder.Encode(state.CurrentTerm)
	encoder.Encode(state.VotedFor)
	encoder.Encode(state.StartIndex) // where the log starts from
	encoder.Encode(state.LogCopy)
	raftstate := w.Bytes()
	//Atomically store the size of the raft state persisted
	atomic.StoreInt64(&rf.raftStateSize, int64(len(raftstate)))
	//We are currently persisting the snaphot too since this is a memory right
	//Will change this to just a single state right later since the snapshot will be in
	//in a different file
	rf.persister.Save(raftstate, state.Snapshot)
	//After saving the state we want to update the matchInd for this peer
	rf.mu.Lock()
	lastInd := state.StartIndex + len(state.LogCopy) - 1
	if rf.matchIndex[rf.me] < lastInd {
		rf.matchIndex[rf.me] = lastInd
		//Check if there are new values to commit
		//rf.mu.Unlock()
		rf.triggerCommitter()
		//return
	}
	rf.mu.Unlock()

}

func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	encoder := labgob.NewEncoder(w)
	encoder.Encode(rf.CurrentTerm)
	encoder.Encode(rf.VotedFor)
	encoder.Encode(rf.StartIndex) // where the log starts from
	encoder.Encode(rf.Log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var startIndex int
	var log []LogEntry
	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&startIndex) != nil ||
		decoder.Decode(&log) != nil {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.CurrentTerm = currentTerm
	rf.VotedFor = votedFor
	rf.StartIndex = startIndex         //Load the start index
	rf.commitIndex = int32(startIndex) // Start with this as committed
	rf.lastApplied = int32(startIndex)
	rf.Log = log
	//Load the snapshot
	rf.snapshot = rf.persister.ReadSnapshot()
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// Call this with lock held: This created the state for persistence
// when snapshotting is called. Before calling this make sure the log is cropped appropriately
func (rf *Raft) createSnapshotArgs() SnapshotRequest {
	logCopy := make([]LogEntry, len(rf.Log))
	copy(logCopy, rf.Log)
	snapshotArgs := SnapshotRequest{
		Snapshot:    rf.snapshot,
		VotedFor:    rf.VotedFor,
		CurrentTerm: rf.CurrentTerm,
		StartIndex:  rf.StartIndex,
		LogCopy:     logCopy,
		Done:        make(chan bool),
	}
	return snapshotArgs

}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	// We already have the snapshot or we cannot snapshot since the index is greater
	if index <= rf.StartIndex || index > int(rf.commitIndex) {
		rf.mu.Unlock()
		return
	}
	raftDPrintf("me=%d Snapshot(index=%d): before StartIndex=%d commit=%d applied=%d loglen=%d snapBytes=%d",
		rf.me, index, rf.StartIndex, rf.commitIndex, rf.lastApplied, len(rf.Log), len(snapshot))
	if index > int(rf.lastApplied) {
		rf.lastApplied = int32(index)
	}
	sliceIndex := index - rf.StartIndex
	term := rf.Log[sliceIndex].Term
	//We need to now trim the log and store the dummy first entry with the same term and index as
	//the original log
	oldLog := rf.Log
	rf.Log = []LogEntry{{Cmd: nil, Term: term}}
	rf.Log = append(rf.Log, oldLog[sliceIndex+1:]...)
	rf.StartIndex = index
	rf.snapshot = snapshot
	//Change the nextIndex if it is lower than the start index
	for i := range rf.nextIndex {
		if rf.nextIndex[i] <= rf.StartIndex {
			rf.nextIndex[i] = rf.StartIndex + 1
		}
	}
	//PERSISTENCE_REFACTOR: Send snapshot trigger
	//First make sure we write the persist state to the new value : We can change this in the future
	rf.createStatePersistenceArgs()
	snapshotArgs := rf.createSnapshotArgs()
	rf.mu.Unlock()
	//This is to unblock rms goroutine so that snapshotting can happen asynchronously
	go func() {
		rf.snapshotCh <- snapshotArgs

		// Wait for the persistence loop to finish writing.
		// This ensures correct ordering but does NOT block the Raft lock.
		<-snapshotArgs.Done
	}()

	//rf.persist()
	raftDPrintf("me=%d Snapshot(index=%d): after StartIndex=%d commit=%d applied=%d loglen=%d",
		rf.me, index, rf.StartIndex, rf.commitIndex, rf.lastApplied, len(rf.Log))

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
	// Also stale if we've already applied past this point. Installing it would roll
	// back the service state machine (and can violate linearizability).
	if request_args.LastIncludedIndex <= int(rf.lastApplied) {
		raftDPrintf("me=%d InstallSnapshot(idx=%d): stale (lastApplied=%d StartIndex=%d) - ignore",
			rf.me, request_args.LastIncludedIndex, rf.lastApplied, rf.StartIndex)
		rf.mu.Unlock()
		return
	}
	//Now update the log and snapshot state
	//Check if the index and term match
	lastLogIndex := rf.StartIndex + len(rf.Log) - 1
	if request_args.LastIncludedIndex <= lastLogIndex {
		cut := request_args.LastIncludedIndex - rf.StartIndex
		//snapshot matches with a prefix so keep the log with the new term
		if rf.Log[cut].Term == request_args.LastIncludedTerm {
			rf.Log = rf.Log[cut:]
			rf.Log[0].Cmd = nil // dummy term
		} else {
			//Conflict at this point so erase everything
			rf.Log = []LogEntry{{Cmd: nil, Term: request_args.LastIncludedTerm}}
		}
	} else {
		//This supersedes the entire log so need to completely replace
		rf.Log = []LogEntry{{Cmd: nil, Term: request_args.LastIncludedTerm}}
	}

	//Replace the shapshot
	rf.snapshot = request_args.Data
	rf.StartIndex = request_args.LastIncludedIndex
	//Store the max for commit index and lastApplied
	rf.commitIndex = max(rf.commitIndex, int32(rf.StartIndex))
	rf.lastApplied = max(rf.lastApplied, int32(rf.StartIndex))

	//PERSISTENCE_REFACTOR: Persist the new snapshot and the new states if any
	//First make sure we update the persistence args
	rf.createStatePersistenceArgs()
	snapshotArgs := rf.createSnapshotArgs()
	//Send to the top
	applymessage := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.snapshot,
		SnapshotTerm:  request_args.LastIncludedTerm,
		SnapshotIndex: request_args.LastIncludedIndex,
	}

	rf.mu.Unlock()
	//PERSISTENCE_REFACTOR: Change the persistence code here
	rf.snapshotCh <- snapshotArgs
	//Wait till this is done
	<-snapshotArgs.Done
	//rf.persist()
	raftDPrintf("me=%d InstallSnapshot(idx=%d): after StartIndex=%d commit=%d applied=%d loglen=%d",
		rf.me, request_args.LastIncludedIndex, rf.StartIndex, rf.commitIndex, rf.lastApplied, len(rf.Log))
	//Send the updated message back
	rf.applyMu.Lock()
	if !rf.applyClosed {
		rf.applyCh <- applymessage
	}
	rf.applyMu.Unlock()

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
	//lastIndex := rf.StartIndex + len(rf.Log) - 1
	//I will currently force the leader to be part of the majority.This is for snapshotting
	// correctly
	lastIndex := rf.matchIndex[rf.me]
	for newcommitInd := lastIndex; newcommitInd > int(rf.commitIndex); newcommitInd-- {
		count := 0
		for r := range rf.matchIndex {
			if rf.matchIndex[r] >= newcommitInd {
				count += 1
			}
		}
		//Check if this is valid commitIndex and belongs to the current term
		if count > size/2 && rf.Log[newcommitInd-rf.StartIndex].Term == rf.CurrentTerm {
			rf.commitIndex = int32(newcommitInd)
			//Broadcast everytime the commitIndex changes
			rf.commitCond.Broadcast()
			break
		}
	}

}

// Call this without any lock
func (rf *Raft) persistSynchronously() {
	//log.Println("Server", rf.me, "is persisting state for term with the following details", rf.CurrentTerm, rf.StartIndex, rf.VotedFor, len(rf.Log), len(rf.snapshot))
	//rf.triggerPersistence()
	//rf.mu.Unlock()
	doneCh := make(chan bool)
	rf.persistWaitch <- doneCh
	//Wait for doneCh
	<-doneCh
	//log.Println("Server", rf.me, "is done persisting state for term", rf.CurrentTerm)
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	// Send append entries
	rf.mu.Lock()
	//defer rf.mu.Unlock()
	// If this Raft instance has been killed, ignore the RPC.
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
		//rf.persist()
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
		//rf.persist()
		voteGranted = true
		//return
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
	// startIndex=2 length of log is 2 the match index is 2+2-1 = 3
	//PERSISTENCE_REFACTOR: make sure we are not setting this here since we are not sure if the sync is succesfull yet
	//rf.matchIndex[rf.me] = rf.StartIndex + len(rf.Log) - 1
	index = rf.StartIndex + len(rf.Log) - 1
	term = rf.CurrentTerm
	//PERSISTENCE_REFACTOR(TODO):Make sure we add logic to update nextIndex properly
	//Synchronous call to persist before sending the output

	//PERSISTENCE_REFACTOR:Make the call asynchronous send a signal to the channel
	rf.createStatePersistenceArgs()
	rf.sendTrigger()
	//rf.persist()
	//Send appendRPC to the followers
	go rf.sendAppendRPC()

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	rf.commitCond.Broadcast()
	rf.mu.Unlock()

	// Close applyCh so the service/RSM can stop its loops (required by tests).
	rf.applyMu.Lock()
	if !rf.applyClosed {
		close(rf.applyCh)
		rf.applyClosed = true
	}
	rf.applyMu.Unlock()
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
	//defer rf.mu.Unlock()
	//Make sure you dont continue if this was killed
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
	// If term is higher, update term and clear vote. If term is equal, we should
	// still step down to follower (a leader/candidate that receives AppendEntries
	// for the current term must become follower).
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
	//Ignore this since you are sending an index smaller than my startIndex, this is definetely stale.Since
	//you are the leader I already moved because of you or some other leader so  i should match before this
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
	//Update heartbeat
	reply.Success = true
	rf.mu.Unlock()

}
func (rf *Raft) SendInstallSnapshot(server int, args *SnapshotRequestArgs, reply *SnapshotReplyArgs) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// This is called with rf.mu held
func (rf *Raft) sendSnapshotTo(server int) bool {
	snapshotRequestArgs := &SnapshotRequestArgs{
		Term:              rf.CurrentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.StartIndex,
		LastIncludedTerm:  rf.Log[0].Term,
		Data:              rf.snapshot,
	}
	snapshotReplyargs := &SnapshotReplyArgs{}
	rf.mu.Unlock()
	ok := rf.SendInstallSnapshot(server, snapshotRequestArgs, snapshotReplyargs)
	rf.mu.Lock()
	if !ok {
		return false
	}

	//Check if I am still the leader
	if snapshotReplyargs.Term > rf.CurrentTerm {
		rf.state = StateFollower
		rf.VotedFor = -1
		rf.CurrentTerm = snapshotReplyargs.Term
		rf.createStatePersistenceArgs()
		rf.mu.Unlock()
		rf.persistSynchronously()
		return false
	}
	//If its an old snapshot reply, ignore
	if snapshotRequestArgs.Term < rf.CurrentTerm {
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
			//Check if I am still the leader and not killed
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
				entries = []LogEntry{}
			}

			request_args.Entries = entries
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
			if request_args.Term != rf.CurrentTerm || rf.nextIndex[server]-1 != request_args.PrevLogIndex {
				rf.mu.Unlock()
				return
			}
			if reply_args.Term > rf.CurrentTerm {
				rf.state = StateFollower
				rf.VotedFor = -1
				rf.CurrentTerm = reply_args.Term
				//rf.persist()
				//rf.mu.Unlock()
				//Persist this
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
						rf.nextIndex[server] = reply_args.XLen
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
							rf.nextIndex[server] = reply_args.XIndex
						}
					} else {
						rf.nextIndex[server] = rf.StartIndex + lastIndForX + 1 //Update the new index
					}

				}

			} else {
				newMatchIndex := request_args.PrevLogIndex + len(request_args.Entries)
				rf.matchIndex[server] = newMatchIndex
				rf.nextIndex[server] = newMatchIndex + 1
				//We should move this to a constant committer since
				//Everytime there is a success check if the commitInd can be updated
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
	//defer rf.mu.Unlock()
	//Increment the term and start an election
	rf.state = StateCandidate
	rf.CurrentTerm += 1
	rf.VotedFor = rf.me
	//Persist after starting the election: synchronously
	//rf.persist()
	rf.createStatePersistenceArgs()
	rf.mu.Unlock()
	rf.persistSynchronously()
	//Dont need to move during snapshotting since we just need the last index and last term
	rf.mu.Lock()
	//Take the lock again and check if its still a candidate
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
	//Abort election if I am not a candidate in the mean time
	if requestargs == nil {
		return
	}
	//log.Println("Server", rf.me, "is starting an election for term", rf.CurrentTerm)
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
			//defer rf.mu.Unlock()
			//If it is not a candidate anymore ignore everything here
			if rf.state != StateCandidate {
				rf.mu.Unlock()
				return
			}
			//Need to check this again
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
					//rf.persist()
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
					//log.Println("Server", rf.me, "is the leader for term", rf.CurrentTerm)
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
		// Snapshot barrier: Raft must never emit CommandValid messages for indices
		// that are no longer in the log (<= StartIndex). Ensure lastApplied is at
		// least StartIndex before applying committed entries.
		if rf.lastApplied < int32(rf.StartIndex) {
			rf.lastApplied = int32(rf.StartIndex)
		}
		if rf.lastApplied >= rf.commitIndex {
			rf.commitCond.Wait()
		}
		for !rf.killed() && rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			if rf.lastApplied < int32(rf.StartIndex) {
				panic("Last applied is less than start index, " + strconv.Itoa(int(rf.StartIndex)) + " got: " + strconv.Itoa(int(rf.lastApplied)))
			}
			//%d and commit index is %d and start index is %d\n", rf.lastApplied, rf.commitIndex, rf.StartIndex)
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.Log[rf.lastApplied-int32(rf.StartIndex)].Cmd,
				CommandIndex: int(rf.lastApplied),
			}
			rf.mu.Unlock()
			rf.applyMu.Lock()
			if !rf.applyClosed {
				rf.applyCh <- msg
			}
			rf.applyMu.Unlock()
			rf.mu.Lock()
		}
		rf.mu.Unlock()
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
	//Create a new persistence channel
	rf.persistenceTriggerch = make(chan struct{}, 1)
	//Create a snapshot persistence channel
	rf.snapshotCh = make(chan SnapshotRequest, 1)
	// Synchronous persistence wait channel (used by persistSynchronously()).
	rf.persistWaitch = make(chan chan bool)
	// StartIndex with snapshot compaction: This is the index from which the log is present
	rf.StartIndex = 0
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	//Start the persistence loop
	go rf.persistenceLoop()
	// start ticker goroutine to start elections
	go rf.ticker()
	//Start committer to periodically add the committed values to the state machine
	go rf.committer()

	return rf
}
