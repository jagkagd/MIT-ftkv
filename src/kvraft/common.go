package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrBadNet      = "ErrBadNet"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key    string
	Value  string
	Method string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ServerNum int
	ClientId  int64
	RequestId int64
	MsgId     int
}

type PutAppendReply struct {
	Err       Err
	Value     string
	RequestId int64
	MsgId     int
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ServerNum int
	ClientId  int64
	RequestId int64
	MsgId     int
}

type GetReply struct {
	Err       Err
	Value     string
	RequestId int64
	MsgId     int
}
