package shardkv

//
// Sharded key/Value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                  = "OK"                  // 成功
	ErrNoKey            = "ErrNoKey"            // 不存在这个Key
	ErrWrongGroup       = "ErrWrongGroup"       // 找错了Group
	ErrWrongLeader      = "ErrWrongLeader"      // 找错了Leader，当前server为follower
	ShardNotArrived     = "ShardNotArrived"     // 由于发生了Shard迁移，但是Shard还未被发送过来
	ConfigNotArrived    = "ConfigNotArrived"    // 配置发生了更新，但是还没有达到当前的服务器
	ErrInconsistentData = "ErrInconsistentData" // 数据前后不一致
	ErrOverTime         = "ErrOverTime"         // 超时
)

const (
	PutType         = "Put"
	AppendType      = "Append"
	GetType         = "Get"
	UpConfigType    = "UpConfig"
	AddShardType    = "AddShard"
	RemoveShardType = "RemoveShard"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Clientid int64 // 客户端Id
	SeqId    int   // 请求序列号
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Clientid int64 // 客户端Id
	SeqId    int   // 请求序列号
}

type GetReply struct {
	Err   Err
	Value string
}

type SendShardArg struct {
	LastAppliedSeqId map[int64]int // 不同客户端最新的请求序列号，用于在发生分片/分区迁移之后，将分片/分区进行添加完成时，需要更新当前servers对应的客户端请求序列号，避免相同的请求序列重复请求
	ShardId          int           // 分片的Id
	Shard            Shard         // 在某个版本配置信息下的分片内容
	ClientId         int64         // 客户端Id
	SeqId            int           // 请求分片/分区迁移时是对应的版本Id，而不是客户端的请求序列号，用作与最新的版本号做对比，根据不同的比较结果做不同的处理
}

type AddShardReply struct {
	Err Err
}
