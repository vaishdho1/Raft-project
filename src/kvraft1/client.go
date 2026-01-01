package kvraft

import (
	"raftkv/kvsrv1/rpc"
	kvtest "raftkv/kvtest1"
	tester "raftkv/tester1"
	"time"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	lastLeader int
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers, lastLeader: 0}
	// You'll have to add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	args := rpc.GetArgs{Key: key}
	for {
		for off := 0; off < len(ck.servers); off++ {
			curLeader := (off + ck.lastLeader) % len(ck.servers)
		reply := rpc.GetReply{}
			ok := ck.clnt.Call(ck.servers[curLeader], "KVServer.Get", &args, &reply)
			if !ok || reply.Err == rpc.ErrWrongLeader {
			continue
		}
			ck.lastLeader = curLeader
		if reply.Err == rpc.ErrNoKey {
			return "", 0, reply.Err
		}
		if reply.Err == rpc.OK {
			return reply.Value, reply.Version, reply.Err
		}

		}
		time.Sleep(20 * time.Millisecond)
	}

}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	// We should return ErrMaybe only if an earlier RPC might have been processed
	// successfully but its reply was lost (i.e., an "uncertain" outcome).
	uncertain := false
	for {

		for off := 0; off < len(ck.servers); off++ {
			curLeader := (off + ck.lastLeader) % len(ck.servers)
			reply := rpc.PutReply{}
			ok := ck.clnt.Call(ck.servers[curLeader], "KVServer.Put", &args, &reply)
			if !ok {
				uncertain = true
				continue
			}
			if reply.Err == rpc.ErrWrongLeader {
				// We had to retry against another server. With leadership churn,
				// this outcome can still be "uncertain" (the request may have been
				// accepted/committed even if this server reports wrong leader).
				uncertain = true
				continue
			}
			ck.lastLeader = curLeader
			if reply.Err == rpc.OK {
				return reply.Err
			}
			if reply.Err == rpc.ErrVersion {
				if uncertain {
					return rpc.ErrMaybe
				}
				return rpc.ErrVersion
			}
			// Any other error: keep trying.
		}
		time.Sleep(20 * time.Millisecond)
	}
}
