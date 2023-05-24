package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.分片/分区
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number，配置信息的版本编号
	Shards [NShards]int     // shard -> gid，所有分片对应的组信息
	Groups map[int][]string // gid -> servers[]，每个组对应的server信息（一个组对应了一个raft集群，一个raft集群对应了多个分片）
}

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// 加入一个新的组，意味着多了一个raft集群
type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings

	// 在处理join时需要
	SeqId    int
	ClientId int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

// 移除一个组，意味着少了一个raft集群
type LeaveArgs struct {
	GIDs []int

	// 在处理leave时需要
	SeqId    int
	ClientId int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

// 将分片从某个组中移动到另一个组中或者是为指定的分片，分配指定的组
type MoveArgs struct {
	Shard int
	GID   int

	// 在处理move时需要
	SeqId    int
	ClientId int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

// 查询特定版本号的配置信息
type QueryArgs struct {
	Num int // desired config number

	// 在处理query时需要
	SeqId    int
	ClientId int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
