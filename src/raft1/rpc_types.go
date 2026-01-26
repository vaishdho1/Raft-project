package raft

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	Votegranted bool
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
