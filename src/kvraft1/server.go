package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"raftkv/kvraft1/rsm"
	"raftkv/kvsrv1/rpc"
	"raftkv/labgob"
	"raftkv/labrpc"
	tester "raftkv/tester1"
)

type Data struct {
	Value   string
	Version rpc.Tversion
}
type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	//Request object Operation
	mu    sync.Mutex
	store map[string]*Data
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch r := req.(type) {
	case rpc.GetArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		v, ok := kv.store[r.Key]
		//The key is not present
		if !ok {
			return rpc.GetReply{Value: "", Version: 0, Err: rpc.ErrNoKey}
		}
		return rpc.GetReply{Value: v.Value, Version: v.Version, Err: rpc.OK}
	case rpc.PutArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		v, ok := kv.store[r.Key]
		if !ok {
			if r.Version != 0 {
				return rpc.PutReply{Err: rpc.ErrVersion}
			}
			kv.store[r.Key] = &Data{Value: r.Value, Version: 1}
			return rpc.PutReply{Err: rpc.OK}
		}
		//There is a version mismatch
		if r.Version != v.Version {
			return rpc.PutReply{Err: rpc.ErrVersion}
		}
		//If the same version, do the put operation and return it as success
		kv.store[r.Key] = &Data{Value: r.Value, Version: v.Version + 1}
		return rpc.PutReply{Err: rpc.OK}
	default:
		log.Fatalf("KVServer.DoOp: unexpected req type %T", req)
		return nil
	}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	w := new(bytes.Buffer)
	encoder := labgob.NewEncoder(w)
	kv.mu.Lock()
	encoder.Encode(kv.store)
	kv.mu.Unlock()
	snapshotstate := w.Bytes()
	return snapshotstate
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	r := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(r)
	var store map[string]*Data
	if decoder.Decode(&store) != nil {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.store = store
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		reply.Value = ""
		reply.Version = 0
		return
	} else {
		rep := res.(rpc.GetReply)
		*reply = rep
	}

}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)

	err, res := kv.rsm.Submit(*args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	rep := res.(rpc.PutReply)
	reply.Err = rep.Err

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}
	//kv.Restore(persister.ReadSnapshot())
	kv.store = make(map[string]*Data) // Initialize before RSM (Restore will overwrite if snapshot exists)

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	return []tester.IService{kv, kv.rsm.Raft()}
}
