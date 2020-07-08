package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"
	"sync"
	"time"

	"../labrpc"
)

const (
	ChangeLeaderInterval = time.Millisecond * 20
)

type ClerkOp struct {
	Method    string
	Key       string
	Value     string
	RequestId int64
	ClientId  int64
}

type ClerkRep struct {
	Method    string
	Key       string
	Value     string
	RequestId int64
	ClientId  int64
}

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu                   sync.Mutex
	leaderId             int
	cmdsCh               chan ClerkOp
	repsCh               chan ClerkRep
	changeLeaderInterval time.Duration
	clientId             int64
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
	ck.leaderId = 0
	ck.cmdsCh = make(chan ClerkOp)
	ck.repsCh = make(chan ClerkRep)
	ck.clientId = nrand()
	ck.changeLeaderInterval = time.Duration(500 * time.Millisecond)
	go ck.processCmd()
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
	ck.cmdsCh <- ClerkOp{
		Key:       key,
		Value:     "",
		Method:    "Get",
		RequestId: nrand(),
		ClientId:  ck.clientId,
	}
	reply := <-ck.repsCh
	return reply.Value
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
	ck.cmdsCh <- ClerkOp{
		Key:       key,
		Value:     value,
		Method:    op,
		RequestId: nrand(),
		ClientId:  ck.clientId,
	}
	<-ck.repsCh
	return
}

func (ck *Clerk) processCmd() {
	for op := range ck.cmdsCh {
		var val string
		DPrintf("processCmd %v", op)
		switch op.Method {
		case "Get":
			val = ck.processGet(op)
		case "Put":
			ck.processPutAppend(op)
			val = ""
		case "Append":
			ck.processPutAppend(op)
			val = ""
		default:
			log.Fatalf("Unknow method %v", op.Method)
		}
		ck.repsCh <- ClerkRep{
			Method:    op.Method,
			Key:       op.Key,
			Value:     val,
			RequestId: op.RequestId,
			ClientId:  op.ClientId,
		}
	}
}

func (ck *Clerk) processGet(op ClerkOp) string {
	msgId := 0
	leaderId := ck.leaderId
	for {
		msgId++
		reply := GetReply{}
		args := GetArgs{
			Key:       op.Key,
			ServerNum: len(ck.servers),
			ClientId:  op.ClientId,
			RequestId: op.RequestId,
			MsgId:     msgId,
		}
		ok := ck.servers[leaderId].Call("KVServer.Get", &args, &reply)
		if !ok {
			time.Sleep(ck.changeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}
		if reply.RequestId != op.RequestId || reply.MsgId != msgId {
			continue
		}
		switch reply.Err {
		case OK:
			ck.leaderId = leaderId
			return reply.Value
		case ErrNoKey:
			ck.leaderId = leaderId
			return reply.Value
		case ErrWrongLeader:
			time.Sleep(ck.changeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		default:
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}
	}
}

func (ck *Clerk) processPutAppend(op ClerkOp) {
	msgId := 0
	leaderId := ck.leaderId
	for {
		msgId++
		// if msgId > 10 {
		// 	log.Fatalf("Wrong")
		// }
		reply := PutAppendReply{}
		args := PutAppendArgs{
			Method:    op.Method,
			Key:       op.Key,
			Value:     op.Value,
			ServerNum: len(ck.servers),
			RequestId: op.RequestId,
			MsgId:     msgId,
			ClientId:  op.ClientId,
		}
		ok := ck.servers[leaderId].Call("KVServer.PutAppend", &args, &reply)
		// log.Printf("%v %v", ok, reply)
		if !ok {
			time.Sleep(ck.changeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		}
		if reply.RequestId != op.RequestId || reply.MsgId != msgId {
			continue
		}
		switch reply.Err {
		case OK:
			ck.leaderId = leaderId
			return
		case ErrWrongLeader:
			time.Sleep(ck.changeLeaderInterval)
			leaderId = (leaderId + 1) % len(ck.servers)
			continue
		default:
			DPrintf("Other Err: %v", reply.Err)
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
