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

const Debug = 0

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
	ServerNum int
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
	repsCh      map[int64](chan ServerRep)
	selected    bool
	waitTime    int
	lastApplied map[int64]int64

	killedCh chan bool
}

func (kv *KVServer) waitCmd(op ServerOp) ServerRep {
	kv.mu.Lock()
	var ch chan ServerRep
	if _, ok := kv.repsCh[op.RequestId]; !ok {
		ch = make(chan ServerRep, 1)
		kv.repsCh[op.RequestId] = ch
	}
	kv.mu.Unlock()
	var reply ServerRep
	kv.cmdsCh <- op
	timeDuration := time.Duration(kv.waitTime*int(math.Ceil(float64(op.MsgId)/float64(op.ServerNum)))) * time.Millisecond
	timer := time.NewTimer(timeDuration)
	timer.Reset(timeDuration)
	defer timer.Stop()
	select {
	case <-kv.killedCh:
		return ServerRep{}
	case <-timer.C:
		kv.removeCh(op.RequestId)
		reply.Err = ErrBadNet
		return reply
	case rep := <-ch:
		kv.removeCh(op.RequestId)
		reply.Err = rep.Err
		reply.Value = rep.Value
		return reply
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("Server Get %v", args)
	reply.RequestId = args.RequestId
	reply.MsgId = args.MsgId
	op := ServerOp{
		Method:    "Get",
		Key:       args.Key,
		Value:     "",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		MsgId:     args.MsgId,
		ServerNum: args.ServerNum,
	}
	res := kv.waitCmd(op)
	reply.Err = res.Err
	reply.Value = res.Value
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("Server PutAppend %v", args)
	reply.RequestId = args.RequestId
	reply.MsgId = args.MsgId
	op := ServerOp{
		Method:    args.Method,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
		MsgId:     args.MsgId,
		ServerNum: args.ServerNum,
	}
	res := kv.waitCmd(op)
	reply.Err = res.Err
	return
}

func (kv *KVServer) removeCh(id int64) {
	kv.mu.Lock()
	delete(kv.repsCh, id)
	kv.mu.Unlock()
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
	kv.repsCh = make(map[int64](chan ServerRep))
	kv.lastApplied = make(map[int64]int64)
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
		kv.mu.Lock()
		_, _, isLeader := kv.rf.Start(cmd)
		// ch := make(chan ServerRep, 1)
		// kv.repsCh[cmd.RequestId] = ch
		kv.mu.Unlock()
		DPrintf("[%v] raftStart %v", isLeader, cmd)
		if !isLeader {
			if _, ok := kv.repsCh[cmd.RequestId]; ok {
				kv.repsCh[cmd.RequestId] <- ServerRep{
					Err:       ErrWrongLeader,
					RequestId: cmd.RequestId,
					MsgId:     cmd.MsgId,
				}
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
			if _, ok := kv.repsCh[msg.RequestId]; ok {
				kv.repsCh[msg.RequestId] <- reply
				DPrintf("reqId %v DB %v", msg.RequestId, kv.db)
			}
		}
	}
}
