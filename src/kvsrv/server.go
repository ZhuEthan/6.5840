package kvsrv

import (
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	maps map[string]string
	// `requestIdMap` maintains the relationship of ClientID->NextRequestID
	requestIdMap map[int64]int64
	// `responseCache` maintains last response of Put/Append call
	responseCache map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.log(args.ClientId, "get() start key=%s", args.Key)
	kv.invalidateCache(args.ClientId)

	if value, ok := kv.maps[args.Key]; ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}

	kv.log(args.ClientId, "get() returns key=%s value=%s", args.Key, reply.Value)

}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.log(args.ClientId, "put() start key=%s value=%s", args.Key, args.Value)

	// if duplicate Put() call is detected, ignore the further calls
	// if the requestId is not expected
	if kv.checkWriteDuplicate(args.ClientId, args.RequestId) {
		kv.log(args.ClientId, "duplicate put() detected key=%v value=%v", args.Key, args.Value)
		return // no need to return old value for Put() call
	}

	// if goes here, we can assume client has known that any previous RPC results are successful.
	// invalidate cache for any previous Put/Append calls to free memory.
	kv.invalidateCache(args.ClientId)

	// perform operations
	kv.maps[args.Key] = args.Value

	// update next expected request id
	kv.updateClientRequestId(args.ClientId)

	// no need to cache response for Put() call

	kv.log(args.ClientId, "put() returns key=%s", args.Key)

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.log(args.ClientId, "append() start key=%s value=%s", args.Key, args.Value)

	prev, ok := kv.maps[args.Key]

	// if duplicate Append() call is detected, ignore the further calls
	// and returns the cached previous value
	if kv.checkWriteDuplicate(args.ClientId, args.RequestId) {
		kv.log(args.ClientId, "duplicate append() detected key=%v value=%v", args.Key, args.Value)

		// return prev value
		reply.Value = kv.responseCache[args.ClientId]

		kv.log(args.ClientId, "ignore duplicate append() and returns key=%s prevValue=%s", args.Key, reply.Value)

		return
	}

	// if goes here, we can assume client has known that any previous RPC results are successful.
	// invalidate cache for any previous Put/Append calls to free memory.
	kv.invalidateCache(args.ClientId)

	if ok {
		kv.maps[args.Key] = fmt.Sprintf("%s%s", prev, args.Value)
	} else {
		kv.maps[args.Key] = args.Value
	}
	reply.Value = prev

	kv.cacheResponse(args.ClientId, reply.Value)
	kv.updateClientRequestId(args.ClientId)

	kv.log(args.ClientId, "append() returns key=%s prevValue=%s afterValue=%s", args.Key, reply.Value, kv.maps[args.Key])

}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.maps = make(map[string]string)
	kv.requestIdMap = make(map[int64]int64)
	kv.responseCache = make(map[int64]string)

	// You may need initialization code here.
	return kv
}

func (kv *KVServer) log(clientId int64, message string, v ...any) {
	if !ENABLE_DEBUG_LOG {
		return
	}

	pc, _, _, _ := runtime.Caller(1)
	details := runtime.FuncForPC(pc)
	items := strings.Split(details.Name(), ".")
	methodName := items[len(items)-1]

	log.Printf(fmt.Sprintf("[Server] clientId=%d method=%s message=%s", clientId, methodName, message), v...)
}

func (kv *KVServer) invalidateCache(clientId int64) {
	kv.log(clientId, "invalidate response cache value=%s", kv.responseCache[clientId])
	delete(kv.responseCache, clientId)
}

func (kv *KVServer) checkWriteDuplicate(clientId int64, requestId int64) bool {
	currentRequestId, ok := kv.requestIdMap[clientId]
	kv.log(clientId, "incoming=%v expected=%v match=%v", requestId, currentRequestId, currentRequestId == requestId)

	if !ok {
		return false
	}
	return currentRequestId != requestId
}

func (kv *KVServer) updateClientRequestId(clientId int64) {
	currentRequestId, ok := kv.requestIdMap[clientId]
	if !ok {
		kv.requestIdMap[clientId] = 1
	} else {
		kv.requestIdMap[clientId] = currentRequestId + 1
	}
	kv.log(clientId, "update from=%v to=%v", currentRequestId, kv.requestIdMap[clientId])
}

func (kv *KVServer) cacheResponse(clientId int64, value string) {
	kv.log(clientId, "cache response value=%s", value)
	kv.responseCache[clientId] = value
}
