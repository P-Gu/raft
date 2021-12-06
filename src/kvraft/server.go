package kvraft

import (
	"fmt"
	"kvstore/labgob"
	"kvstore/labrpc"
	"kvstore/raft"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false
const ConsensusTimeout = 100
var cmdtype = map[string] int{"Get": 0, "Put": 1, "Append": 2}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	Key string
	Value string
	CmdType int
	ClientId int64
	RequestId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvs map[string]string // kv pairs
	waitApplyCh map[int]chan Op
	lastRequestId map[int64]int64

	durs []time.Duration
	timemu sync.Mutex
}

func(kv* KVServer) isRequestDuplicate(ClientId int64, RequestId int64) bool{
	kv.mu.Lock()
	defer kv.mu.Unlock()
	lastRequestId, exist := kv.lastRequestId[ClientId]

	if !exist || lastRequestId < RequestId{
		return false
	} else {
		return true
	}
}


func (kv*KVServer) ExecuteGetOp(op Op) (string, bool){
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if op.CmdType != cmdtype["Get"] {
		panic("ExecuteGetOp can only be used for Get Op")
	}
	value, exist := kv.kvs[op.Key]
	if kv.lastRequestId[op.ClientId] < op.RequestId{
		kv.lastRequestId[op.ClientId] = op.RequestId
	}

	if !exist{
		return "", false
	} else {
		return value, true
	}
}

func (kv* KVServer) ExecutePutOp(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if op.CmdType != cmdtype["Put"]{
		panic("ExecutePutOp can only be used for Put Op")
	}
	DPrintf("[server %d]: executing Put op %v", kv.me, op)
	key, value, clientId, requestId := op.Key, op.Value, op.ClientId, op.RequestId
	kv.kvs[key] = value
	kv.lastRequestId[clientId] = requestId
	DPrintf("[server %d]: now kvs[%v]=%v", kv.me, key, kv.kvs[key])
}

func (kv* KVServer) ExecuteAppendOp(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()


	if op.CmdType != cmdtype["Append"]{
		panic("ExecuteAppendOp can only be used for Append Op")
	}
	key, newvalue, clientId, requestId := op.Key, op.Value, op.ClientId, op.RequestId

	DPrintf("[server %d]: executing Append op %v", kv.me, op)
	oldvalue, exist := kv.kvs[key]
	if exist{
		kv.kvs[key] = oldvalue + newvalue
	} else {
		kv.kvs[key] = newvalue
	}
	DPrintf("[server %d]: now kvs[%v]=%v", kv.me, key, kv.kvs[key])
	kv.lastRequestId[clientId] = requestId
}

const TimeDebug = false
func (kv*KVServer) UpdateDurs(dur time.Duration){
	if TimeDebug{
		kv.timemu.Lock()
		defer kv.timemu.Unlock()
		kv.durs = append(kv.durs, dur)

		sum := time.Duration(0)
		for i:=0; i<len(kv.durs);i++{
			sum += kv.durs[i]
		}
		avg := float64(sum.Milliseconds()) / float64(len(kv.durs))
		fmt.Printf("[server %v]: avg duration=%v\n", kv.me, avg)
	}

}
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("[server %d]: Get: receives a req: [Key=%v, ClientId=%v, RequestId=%v]",
		kv.me, args.Key, args.ClientId, args.RequestId)
	if kv.killed(){
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	_, isLeader := kv.rf.GetState()
	if !isLeader{
		DPrintf("[server %d]: Get: wrong leader: [Key=%v, ClientId=%v, RequestId=%v]",
			kv.me, args.Key, args.ClientId, args.RequestId)
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}

	op := Op{args.Key, "", cmdtype["Get"], args.ClientId, args.RequestId}
	DPrintf("[server %d]: Get: trying to achieve consensus for req: [Key=%v, ClientId=%v, RequestId=%v]",
		kv.me, args.Key, args.ClientId, args.RequestId)

	time1 := time.Now()
	raftIndex, _, _ := kv.rf.Start(op)

	kv.mu.Lock()
	chRaftIndex, chExist := kv.waitApplyCh[raftIndex]
	if !chExist{
		kv.waitApplyCh[raftIndex] = make(chan Op, 1)
		chRaftIndex = kv.waitApplyCh[raftIndex]
	}
	kv.mu.Unlock()

	select {
	case <-time.After(ConsensusTimeout * time.Millisecond):
		dur := time.Since(time1)
		kv.UpdateDurs(dur)

		if kv.isRequestDuplicate(op.ClientId, op.RequestId) {
			DPrintf("[server %d]: Get: op [Key=%v, ClientId=%v, RequestId=%v] executed",
				kv.me, args.Key, args.ClientId, args.RequestId)
			value, exist := kv.ExecuteGetOp(op)
			if exist {
				reply.Err = OK
				reply.Value = value
				reply.WrongLeader = false
			} else {
				reply.Err = ErrNoKey
				reply.Value = ""
				reply.WrongLeader = false
			}

		} else {
			DPrintf("[server %d]: Get: op [Key=%v, ClientId=%v, RequestId=%v] wrong leader",
				kv.me, args.Key, args.ClientId, args.RequestId)
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		}

		return
	case raftCommittedOp := <-chRaftIndex:
		{
			dur := time.Since(time1)
			kv.UpdateDurs(dur)
			if raftCommittedOp.ClientId == op.ClientId && raftCommittedOp.RequestId == op.RequestId { // is this condition necessary?
				DPrintf("[server %d]: Get: op [Key=%v, ClientId=%v, RequestId=%v] executed",
					kv.me, args.Key, args.ClientId, args.RequestId)
				value, exist := kv.ExecuteGetOp(op)
				if exist {
					reply.Err = OK
					reply.Value = value
					reply.WrongLeader = false
				} else {
					reply.Err = ErrNoKey
					reply.Value = ""
					reply.WrongLeader = false
				}
			} else {
				panic("Unknown Error")
			}
		}
	}

	kv.mu.Lock()
	delete(kv.waitApplyCh, raftIndex)
	kv.mu.Unlock()
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// if kvserver is killed or not the raft leader, return
	DPrintf("[server %d]: PutAppend: receives a req: [Key=%v, Value=%v, Op=%v, ClientId=%v, RequestId=%v]",
		kv.me, args.Key, args.Value, args.Op, args.ClientId, args.RequestId)
	if kv.killed(){
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		return
	}
	_, isLeader := kv.rf.GetState()
	DPrintf("[server %d]: isLeader=%v", kv.me, isLeader)
	if !isLeader{
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true

		DPrintf("[server %d]: PutAppend: not leader, return: [Err=%v, WrongLeader=%v, LeaderId=%d]",
			kv.me, reply.Err, reply.WrongLeader, reply.LeaderId)
		return
	}

	op := Op{args.Key, args.Value, cmdtype[args.Op], args.ClientId, args.RequestId}
	DPrintf("[server %d]: PutAppend: leader, trying to achieve consensus for req: [Key=%v, Value=%v, Op=%v, ClientId=%v, RequestId=%v]",
		kv.me, args.Key, args.Value, args.Op, args.ClientId, args.RequestId)
	time1 := time.Now()
	raftIndex, _, _ := kv.rf.Start(op)

	kv.mu.Lock()
	chRaftIndex, chExist := kv.waitApplyCh[raftIndex]
	if !chExist{
		kv.waitApplyCh[raftIndex] = make(chan Op, 1)
		chRaftIndex = kv.waitApplyCh[raftIndex]
	}
	kv.mu.Unlock()

	// wait on this channel until the Raft notifies that the op can be applied
	select {
	case <-time.After(ConsensusTimeout * time.Millisecond):
		dur := time.Since(time1)
		kv.UpdateDurs(dur)

		DPrintf("[%d]: consensus timeout", kv.me)
		// timeouts doesn't mean that the op fails to achieve a consensus, double check here
		if kv.isRequestDuplicate(op.ClientId, op.RequestId){
			DPrintf("[server %d]: PutAppend: op [Key=%v, Value=%v, Op=%v, ClientId=%v, RequestId=%v] executed",
				kv.me, args.Key, args.Value, args.Op, args.ClientId, args.RequestId)
			reply.Err = OK
			reply.WrongLeader = false
		} else {
			DPrintf("[server %d]: PutAppend: op [Key=%v, Value=%v, Op=%v, ClientId=%v, RequestId=%v] wrong leader",
				kv.me, args.Key, args.Value, args.Op, args.ClientId, args.RequestId)
			reply.Err = ErrWrongLeader
			reply.WrongLeader = true
		}

	case raftCommittedOp := <-chRaftIndex:
		dur := time.Since(time1)
		kv.UpdateDurs(dur)
		DPrintf("[server %d]: PutAppend: op [Key=%v, Value=%v, Op=%v, ClientId=%v, RequestId=%v] returned",
			kv.me, args.Key, args.Value, args.Op, args.ClientId, args.RequestId)
		if raftCommittedOp.ClientId == op.ClientId && raftCommittedOp.RequestId == op.RequestId{// necessary?
			reply.Err= OK
			reply.WrongLeader = false
			DPrintf("[server %d]: PutAppend: op [Key=%v, Value=%v, Op=%v, ClientId=%v, RequestId=%v] executed",
				kv.me, args.Key, args.Value, args.Op, args.ClientId, args.RequestId)
		} else {
			panic("Unknown Error")
		}
	}

	kv.mu.Lock()
	delete(kv.waitApplyCh, raftIndex)
	kv.mu.Unlock()
	return
}

//
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
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// only used for Op returned by Raft, ie. Put and Append
// Get Op is executed directly without entering Raft
func (kv* KVServer) NotifyFinish(msg raft.ApplyMsg){
	op := msg.Command.(Op)

	// deduplicate in the state machine
	if !kv.isRequestDuplicate(op.ClientId, op.RequestId){
		// executed this op
		switch op.CmdType {
		//case cmdtype["Get"]:
			//kv.ExecuteGetOp(op.Key)
		case cmdtype["Put"]:
			kv.ExecutePutOp(op)
		case cmdtype["Append"]:
			kv.ExecuteAppendOp(op)
		// do nothing for Get
		}
	}

	// return to the RPC handler
	kv.sendMsgToWaitChan(op, msg.CommandIndex)
}



func(kv*KVServer) sendMsgToWaitChan(op Op, logIndex int){
	kv.mu.Lock()
	defer kv.mu.Unlock()

	waitCh, exist := kv.waitApplyCh[logIndex]
	if exist{
		waitCh <- op
	}
	return
}

func (kv *KVServer) ApplyLoop() {
	for kv.killed() == false {
		DPrintf("[server %d]: waiting from applyCh", kv.me)
		msg := <-kv.applyCh
		DPrintf("[server %d]: receives a msg %v of type %T from applyCh", kv.me, msg, msg)
		if msg.CommandValid{
			kv.NotifyFinish(msg)
		}
	}
	//DPrintf("[%d]: ")
	//kv.rf.Kill()
}
//
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
// StartKVServer() must return quickly, so it should not start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	// create a KV server and initialize
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = -1

	// You may need initialization code here.
	kv.kvs = make(map[string]string)
	kv.waitApplyCh = make(map[int]chan Op)
	kv.lastRequestId = make(map[int64]int64)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	//kv.durs = make([]time.Duration)

	// You may need initialization code here.
	DPrintf("[server %d]: we have %v servers in total", kv.me, len(servers))
	go kv.ApplyLoop()
	return kv
}
