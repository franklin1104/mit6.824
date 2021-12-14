package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
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
	killCh           chan bool
	kvs              map[string]string
	msgIds           map[int64]int64
	lastAppliedIndex int
	persister        *raft.Persister
}

func (kv *KVServer) opt(cliId int64, msgId int64, req interface{}) (bool, Err, interface{}) {
	if msgId > 0 && kv.isRepeated(cliId, msgId, false) {
		return true, "", nil
	}
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
		//fmt.Println("out of time")
		return false, ErrTimeout, nil
	}
}

func (kv *KVServer) get(args *GetArgs) string {
	value, ok := kv.kvs[args.Key]
	//fmt.Println(kv.me, "on get", args.Key, ":", value, ":", ok)
	if !ok {
		return ""
	}
	return value
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	ok, err, value := kv.opt(-1, -1, *args)
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
	ok, err, _ := kv.opt(args.CliId, args.MsgId, *args)
	if !ok {
		reply.Err = err
	}
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
	if !applyMsg.CommandValid && applyMsg.SnapshotValid {
		if kv.rf.HandleInstallSnapshot(applyMsg.SnapshotTerm, applyMsg.SnapshotIndex, applyMsg.Snapshot) {
			kv.restoreSnapshot(applyMsg.Snapshot)
			kv.lastAppliedIndex = applyMsg.SnapshotIndex
		}
		return
	}
	kv.lastAppliedIndex = applyMsg.CommandIndex
	op := applyMsg.Command.(Op)
	var resp interface{}
	if args, ok := op.Req.(PutAppendArgs); ok {
		if !kv.isRepeated(args.CliId, args.MsgId, true) {
			kv.putAppend(&args)
		}
		resp = true
	} else {
		args := op.Req.(GetArgs)
		resp = kv.get(&args)
	}
	select {
	case op.Ch <- resp:
	default:
	}

	if kv.needSnapshot() {
		kv.takeSnapshot()
	}
}

func (kv *KVServer) isRepeated(cliId int64, msgId int64, update bool) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastId, ok := kv.msgIds[cliId]
	if ok && lastId >= msgId {
		return true
	} else {
		if update {
			kv.msgIds[cliId] = msgId
		}
		return false
	}
}

func (kv *KVServer) needSnapshot() bool {
	return kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate
}

func (kv *KVServer) takeSnapshot() {
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(kv.msgIds)
	encoder.Encode(kv.kvs)
	snapshot := writer.Bytes()
	kv.rf.HandleInstallSnapshot(kv.rf.GetTermOfIndex(kv.lastAppliedIndex), kv.lastAppliedIndex, snapshot)
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

func (kv *KVServer) restoreSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	reader := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(reader)

	if decoder.Decode(&kv.msgIds) != nil || decoder.Decode(&kv.kvs) != nil {
		DPrintf("server %d fails to restore snapshot", kv.me)
		return
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
	kv.lastAppliedIndex = 0
	kv.persister = persister

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg, 10000)
	kv.kvs = make(map[string]string)
	kv.msgIds = make(map[int64]int64)
	kv.killCh = make(chan bool)

	go kv.eventLoop()
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	return kv
}
