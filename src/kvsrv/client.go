package kvsrv

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"runtime"
	"strings"
	"sync/atomic"

	"6.5840/labrpc"
)

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.

	id        int64
	requestId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.

	ck.id = nrand() % 10000
	ck.requestId = 0

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key: key,
	}
	reply := GetReply{}

	for {
		ck.log("get() start key=%v", key)

		ok := ck.server.Call("KVServer.Get", &args, &reply)
		if ok {
			return reply.Value
		}

		ck.log("get() failure key=%v", key)

	}

}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		ClientId:  ck.id,
		RequestId: atomic.LoadInt64(&ck.requestId),
	}
	reply := PutAppendReply{}

	for {
		ck.log("putAppend() op=%s start key=%v value=%v", op, key, value)

		ok := ck.server.Call("KVServer."+op, &args, &reply)
		if ok {
			atomic.AddInt64(&ck.requestId, 1)
			return reply.Value
		}
		ck.log("putAppend() op=%s failure key=%v", op, key)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}

// Log utility function
func (ck *Clerk) log(format string, v ...any) {
	if !ENABLE_DEBUG_LOG {
		return
	}

	pc, _, _, _ := runtime.Caller(1)
	details := runtime.FuncForPC(pc)
	items := strings.Split(details.Name(), ".")
	methodName := items[len(items)-1]

	log.Printf(fmt.Sprintf("[Client] id=%v method=%s message=%s", ck.id, methodName, format), v...)
}
