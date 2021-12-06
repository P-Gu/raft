package kvraft

import (
	"kvstore/labrpc"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int // which server to send to
	clientId int64
	requestId int64

}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.leaderId = 0
	ck.clientId = nrand()
	// You'll have to add code here.
	ck.requestId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{key, ck.clientId, ck.requestId}
	ck.requestId += 1
	// You will have to modify this function.
	for i:=ck.leaderId;; i=(i+1)%len(ck.servers){
		reply := new(GetReply)
		DPrintf("[client %v]: calling Get: [Key=%v, ClientId=%v, RequestId=%v]", ck.clientId, args.Key, args.ClientId, args.RequestId)

		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)

		DPrintf("[client %v]: get the Get response: [Err=%v, Value=%v, WrongLeader=%v, LeaderId=%v] for req [Key=%v, ClientId=%v, RequestId=%v]",
			ck.clientId, reply.Err, reply.Value, reply.WrongLeader, reply.LeaderId,
			args.Key, args.ClientId, args.RequestId)
		if ok && !reply.WrongLeader{
			ck.leaderId = i
			if reply.Err == ErrNoKey{
				return ""
			} else {
				return reply.Value
			}
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{key, value, op, ck.clientId, ck.requestId}
	ck.requestId += 1
	for i:=ck.leaderId;; i=(i+1)%len(ck.servers){
		reply := new(PutAppendReply)

		DPrintf("[client %v]: calling PutAppend to server %d: Key=%v, Value=%v, Op=%v, ClientId=%v, RequestId=%v",
			ck.clientId, i, args.Key, args.Value, args.Op, args.ClientId, args.RequestId)
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)

		DPrintf("[client %v]: get the PutAppend response from server %d: [Err=%v, WrongLeader=%v, LeaderId=%v] for req [Key=%v, Value=%v, Op=%v, ClientId=%v, RequestId=%v]",
			ck.clientId, i, reply.Err, reply.WrongLeader, reply.LeaderId,
			args.Key, args.Value, args.Op, args.ClientId, args.RequestId)
		if ok && !reply.WrongLeader{
			ck.leaderId = i
			return
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}


