package kvraft

import (
	"sync/atomic"
	"time"

	"raftkv/kvsrv1/rpc"
	kvtest "raftkv/kvtest1"
	tester "raftkv/tester1"
)

const (
	RPCTimeout       = 2 * time.Second
	OperationTimeout = 10 * time.Second
)

var clientInFlight int64

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	lastLeader int
	metrics    MetricsSink
	metricsOn  bool
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers, lastLeader: 0}
	// You'll have to add code here.
	ck.metricsOn = metricsEnabled()
	ck.metrics = getMetricsSink()
	return ck
}

// This is used to wrap the RPC into a timeout so that we can return or stop after the timeout
func (ck *Clerk) timedCall(server int, method string, args any, reply any) bool {

	done := make(chan bool, 1)
	go func() {
		done <- ck.clnt.Call(ck.servers[server], method, args, reply)
	}()
	select {
	case ok := <-done:
		return ok
	case <-time.After(RPCTimeout):
		return false
	}
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
func (ck *Clerk) IncInflightRequests(method string) func() {
	if !ck.metricsOn {
		return func() {}
	}
	ck.metrics.IncRequest(method)
	atomic.AddInt64(&clientInFlight, 1)
	ck.metrics.SetClientInFlight(int(atomic.LoadInt64(&clientInFlight)))
	return func() {
		atomic.AddInt64(&clientInFlight, -1)
		ck.metrics.SetClientInFlight(int(atomic.LoadInt64(&clientInFlight)))
	}
}
func (ck *Clerk) IncFailure(method string, reason string) {
	if !ck.metricsOn {
		return
	}

	ck.metrics.IncFailure(method, reason)
}
func (ck *Clerk) IncRetries(method string) {
	if !ck.metricsOn {
		return
	}
	ck.metrics.IncRetry(method)
}
func (ck *Clerk) ObserveLatency(method string, time time.Duration) {
	if !ck.metricsOn {
		return
	}
	ck.metrics.ObserveLatency(method, time)
}
func (ck *Clerk) SetVersion(method string, version int64) {
	if !ck.metricsOn {
		return
	}
	ck.metrics.SetObservedVersion(method, version)
}
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// You will have to modify this function.
	done := ck.IncInflightRequests("Get")
	defer done()
	start := time.Now()
	args := rpc.GetArgs{Key: key}
	for {
		for off := 0; off < len(ck.servers); off++ {
			curLeader := (off + ck.lastLeader) % len(ck.servers)
			reply := rpc.GetReply{}
			ok := ck.timedCall(curLeader, "KVServer.Get", &args, &reply)
			if !ok || reply.Err == rpc.ErrWrongLeader {
				ck.IncRetries("Get")
				if !ok {
					ck.IncFailure("Get", "rpc_failed")
				} else {
					ck.IncFailure("Get", "wrong_leader")
				}

				continue
			}
			ck.lastLeader = curLeader
			if reply.Err == rpc.ErrNoKey {
				ck.ObserveLatency("Get", time.Since(start))
				return "", 0, reply.Err
			}
			if reply.Err == rpc.OK {
				ck.SetVersion("Get", int64(reply.Version))
				ck.ObserveLatency("Get", time.Since(start))
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
	done := ck.IncInflightRequests("Put")
	defer done()
	start := time.Now()
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	// We should return ErrMaybe only if an earlier RPC might have been processed
	// successfully but its reply was lost (i.e., an "uncertain" outcome).
	uncertain := false
	for {

		for off := 0; off < len(ck.servers); off++ {
			curLeader := (off + ck.lastLeader) % len(ck.servers)
			reply := rpc.PutReply{}
			ok := ck.timedCall(curLeader, "KVServer.Put", &args, &reply)
			if !ok || reply.Err == rpc.ErrWrongLeader {
				ck.IncRetries("Put")
				if !ok {
					ck.IncFailure("Put", "rpc_failed")
				} else {
					ck.IncFailure("Put", "wrong_leader")
				}
				uncertain = true
				continue
			}
			ck.lastLeader = curLeader
			if reply.Err == rpc.OK {
				ck.ObserveLatency("Put", time.Since(start))
				return reply.Err
			}
			if reply.Err == rpc.ErrVersion {
				ck.IncFailure("Put", "version")
				if uncertain {
					return rpc.ErrMaybe
				}
				ck.ObserveLatency("Put", time.Since(start))
				return rpc.ErrVersion
			}
			// Any other error: keep trying.
		}
		time.Sleep(20 * time.Millisecond)
	}
}
