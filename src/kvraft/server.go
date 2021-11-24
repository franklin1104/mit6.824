package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Req interface{}
	Ch  chan interface{}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	killCh chan bool
	kvs    map[string]string
}

func (kv *KVServer) opt(req interface{}) (bool, Err, interface{}) {
	op := Op{
		Req: req,
		Ch:  make(chan interface{}),
	}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return false, ErrWrongLeader, nil
	}
	select {
	case resp := <-op.Ch:
		return true, "", resp
	case <-time.After(time.Millisecond * 1000):
		return false, ErrNoKey, nil
	}
}

func (kv *KVServer) get(args *GetArgs) string {
	value, ok := kv.kvs[args.Key]
	if !ok {
		return ""
	}
	return value
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	ok, err, value := kv.opt(*args)
	if !ok {
		//fmt.Println( "Get Err:" + err)
		reply.Err = err
		return
	}
	//fmt.Println("leader handle op get")
	reply.Value = value.(string)
	return
}

func (kv *KVServer) putAppend(args *PutAppendArgs) {
	if args.Op == OpPut {
		kv.kvs[args.Key] = args.Value
	} else if args.Op == OpAppend {
		value, ok := kv.kvs[args.Key]
		if !ok {
			value = ""
		}
		value += args.Value
		kv.kvs[args.Key] = value
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	ok, err, _ := kv.opt(*args)
	if !ok {
		//fmt.Println("PutAppend Err:" + err)
		reply.Err = err
		return
	}
	//fmt.Println("leader handle op put append")
}

// Kill
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	kv.killCh <- true
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) onApply(applyMsg raft.ApplyMsg) {
	if !applyMsg.CommandValid {
		//TODO: deal with non apply msg
		return
	}

	op := applyMsg.Command.(Op)
	var resp interface{}
	if args, ok := op.Req.(PutAppendArgs); ok {
		kv.putAppend(&args)
		resp = true
	} else {
		args := op.Req.(GetArgs)
		resp = kv.get(&args)
	}
	select {
	case op.Ch <- resp:
	default:
	}
}

func (kv *KVServer) eventLoop() {
	for {
		select {
		case <-kv.killCh:
			return
		case msg := <-kv.applyCh:
			kv.onApply(msg)
		}
	}
}

// StartKVServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//

func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(GetArgs{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 10000)
	kv.kvs = make(map[string]string)
	kv.killCh = make(chan bool)

	go kv.eventLoop()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	return kv
}
