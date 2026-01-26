package raft

import "raftkv/labrpc"

type LabrpcTransport struct {
	peers []*labrpc.ClientEnd
}

func NewLabrpcTransport(peers []*labrpc.ClientEnd) *LabrpcTransport {
	cp := append([]*labrpc.ClientEnd(nil), peers...)
	return &LabrpcTransport{peers: cp}
}

func (tr *LabrpcTransport) NumPeers() int {
	return len(tr.peers)
}

func (tr *LabrpcTransport) AppendEntries(peer int, args *AppendEntriesReqArgs, reply *AppendEntriesResponseArgs) bool {
	ok := tr.peers[peer].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (tr *LabrpcTransport) RequestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := tr.peers[peer].Call("Raft.RequestVote", args, reply)
	return ok
}
func (tr *LabrpcTransport) InstallSnapshot(peer int, args *SnapshotRequestArgs, reply *SnapshotReplyArgs) bool {
	ok := tr.peers[peer].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
