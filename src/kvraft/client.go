package kvraft

import (
	"crypto/rand"
	"kvstore/labrpc"
	"math/big"
	"strconv"
	"sync"
)

type Clerk struct {
	mu      sync.Mutex
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderID int
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
	// You'll have to add code here.
	ck.leaderID = -1
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
	args := GetArgs{key}
	reply := GetReply{}
	ok := ck.servers[ck.leaderID].Call("KVServer.Get", &args, &reply)
	if !ok {
		return "error connecting"
	}
	if reply.Error != "" {
		return string(reply.Error)
	}
	for !reply.IsLeader {
		ck.leaderID, _ = strconv.Atoi(reply.Value)
		ok = ck.servers[ck.leaderID].Call("KVServer.Get", &args, &reply)
		if !ok {
			return "error connecting"
		}
		if reply.Error != "" {
			return string(reply.Error)
		}
	}

	// get the reply from leader server
	ck.mu.Lock()
	defer ck.mu.Unlock()

	// You will have to modify this function.
	return ""
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
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
