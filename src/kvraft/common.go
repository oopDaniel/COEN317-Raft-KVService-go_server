package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Res string

// Put or Append
type PutAppendArgs struct {
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	ClientId int64
	ReqSeqId int
}

type PutAppendReply struct {
	WrongLeader bool
	Res         Res
}

type GetArgs struct {
	Key      string
	ClientId int64
	ReqSeqId int
}

type GetReply struct {
	WrongLeader bool
	Res         Res
	Value       string
}
