package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string // 向服务器添加kv键值对，key
	Value string // 向服务器添加kv键值对，value
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64 // 向服务器发送添加键值对的客户端id
	SeqId    int   // 向服务器发送请求序列号
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	CliendId int64 // 客户端id
	SeqId    int   // 客户端请求序列号
}

type GetReply struct {
	Err   Err
	Value string
}
