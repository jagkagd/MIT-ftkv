package kvraft

import (
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type ServerOp struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Method    string
	Key       string
	Value     string
	ClientId  int64
	RequestId int64
	MsgId     int
}

type Op = ServerOp

type ServerRep struct {
	Value     string
	Err       Err
	ClientId  int64
	RequestId int64
	MsgId     int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	db          map[string]string
	cmdsCh      chan ServerOp
	repsCh      chan ServerRep
	selected    bool
	waitTime    int
	lastApplied map[int64]int64
	notifyChs   map[int64](chan bool)

	killedCh chan bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// log.Printf("Server Get %v", args)
	kv.mu.Lock()
	kv.notifyChs[args.ClientId] = make(chan bool, 1)
	kv.mu.Unlock()
	reply.RequestId = args.RequestId
	reply.MsgId = args.MsgId
	kv.cmdsCh <- ServerOp{
		Method:    "Get",
		Key:       args.Key,
		Value:     "",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		MsgId:     args.MsgId,
	}
	select {
	case <-kv.killedCh:
		return
	case <-time.After(time.Duration(kv.waitTime*int(math.Ceil(float64(args.MsgId)/float64(args.ServerNum)))) * time.Millisecond):
		reply.Err = ErrBadNet
		kv.notifyChs[args.ClientId] <- true
		return
	case rep := <-kv.repsCh:
		if rep.RequestId != args.RequestId {
			reply.Err = ErrBadNet
			return
		}
		reply.Err = rep.Err
		reply.Value = rep.Value
		return
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// log.Printf("Server PutAppend %v", args)
	kv.mu.Lock()
	kv.notifyChs[args.ClientId] = make(chan bool, 1)
	kv.mu.Unlock()
	reply.RequestId = args.RequestId
	reply.MsgId = args.MsgId
	kv.cmdsCh <- ServerOp{
		Method:    args.Method,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		MsgId:     args.MsgId,
	}
	// log.Printf("time %v", kv.waitTime*int(math.Ceil(float64(args.MsgId)/float64(args.ServerNum))))
	select {
	case <-kv.killedCh:
		return
	case <-time.After(time.Duration(kv.waitTime*int(math.Ceil(float64(args.MsgId)/float64(args.ServerNum)))) * time.Millisecond):
		DPrintf("Timeout")
		reply.Err = ErrBadNet
		kv.notifyChs[args.ClientId] <- true
		return
	case rep := <-kv.repsCh:
		if rep.RequestId != args.RequestId { // || rep.MsgId != args.MsgId {
			DPrintf("%v %v %v %v", rep.RequestId, args.RequestId, rep.MsgId, args.MsgId)
			reply.Err = ErrBadNet
			return
		}
		reply.Err = rep.Err
		return
	}
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
	close(kv.killedCh)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
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
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.db = make(map[string]string)
	kv.waitTime = 500
	kv.cmdsCh = make(chan ServerOp)
	kv.repsCh = make(chan ServerRep)
	kv.lastApplied = make(map[int64]int64)
	kv.notifyChs = make(map[int64](chan bool), 1)
	kv.killedCh = make(chan bool)

	go kv.updateDB()
	go kv.raftStart()

	// You may need initialization code here.

	return kv
}

func (kv *KVServer) raftStart() {
	for {
		var cmd ServerOp
		select {
		case <-kv.killedCh:
			return
		case cmd = <-kv.cmdsCh:
		}
		// log.Printf("raftStart %v", cmd)
		_, _, isLeader := kv.rf.Start(cmd)
		if !isLeader {
			kv.repsCh <- ServerRep{
				Err:       ErrWrongLeader,
				RequestId: cmd.RequestId,
				MsgId:     cmd.MsgId,
			}
		}
	}
}

func (kv *KVServer) updateDB() {
	for {
		var command raft.ApplyMsg
		select {
		case <-kv.killedCh:
			return
		case command = <-kv.applyCh:
		}
		_, isLeader := kv.rf.GetState()
		if isLeader {
			DPrintf("updateDB %v", command.Command)
		}
		msg := command.Command.(ServerOp)
		kv.mu.Lock()
		toUpdate := true
		if v, ok := kv.lastApplied[msg.ClientId]; ok {
			if v == msg.RequestId {
				toUpdate = false
			}
		}
		key := msg.Key
		value := msg.Value
		reply := ServerRep{
			Err:       OK,
			RequestId: msg.RequestId,
			MsgId:     msg.MsgId,
		}
		switch msg.Method {
		case "Get":
			if v, ok := kv.db[key]; ok {
				reply.Value = v
			} else {
				reply.Err = ErrNoKey
			}
		case "Put":
			if toUpdate {
				if _, ok := kv.db[key]; ok {
					kv.db[key] = value
				} else {
					kv.db[key] = value
				}
			}
		case "Append":
			if toUpdate {
				if v, ok := kv.db[key]; ok {
					kv.db[key] = v + value
				} else {
					kv.db[key] = value
				}
			}
		}
		if toUpdate {
			kv.lastApplied[msg.ClientId] = msg.RequestId
		}
		kv.mu.Unlock()
		if isLeader {
			select {
			case <-kv.notifyChs[msg.ClientId]:
				continue
			default:
				kv.repsCh <- reply
				DPrintf("DB %v", kv.db)
			}
		}
	}
}
