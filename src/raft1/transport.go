package raft

// This is the interface all networking rpcs need to abode by
type Transport interface {
	NumPeers() int
	RequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool
	AppendEntries(peer int, args *AppendEntriesReqArgs, reply *AppendEntriesResponseArgs) bool
	InstallSnapshot(peer int, request_args *SnapshotRequestArgs, reply_args *SnapshotReplyArgs) bool
}
