package shardkv

//
// client code to talk to a sharded key/Value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import "6.824/labrpc"
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
// 生成shardId，对应着分片/分区数组的下标
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk             // 获取分片控制器的客户端
	config   shardctrler.Config             // 通过分片控制器获取到的最新的配置信息，便于进行查询自己需要访问哪个Group，即哪个raft集群
	make_end func(string) *labrpc.ClientEnd // 知道了是哪个Group之后，需要通过次方法来获取服务器名对应的服务端
	// You will have to modify this struct.

	seqId    int   // 请求的序列号
	clientId int64 // 客户端号
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)                       // 创建一个客户端
	ck.sm = shardctrler.MakeClerk(ctrlers) // 作为客户端向分片控制器发送消息，获取最新的配置信息
	ck.make_end = make_end                 // 通过服务器名来获取服务端的方法
	// You'll have to add code here.

	ck.clientId = nrand()       // 随机生成客户端Id
	ck.seqId = 0                // 初始化序列号
	ck.config = ck.sm.Query(-1) // 获取系统最新的版本配置信息

	return ck
}

//
// fetch the current Value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	ck.seqId++ // 更新请求的序列号
	for {
		args := GetArgs{ // 生成请求参数
			Key:      key,
			Clientid: ck.clientId,
			SeqId:    ck.seqId,
		}
		shard := key2shard(key)                       // 生成shardId
		gid := ck.config.Shards[shard]                // 获取shard分片所在的组，即它对应的raft集群
		if servers, ok := ck.config.Groups[gid]; ok { // 获取对应的raft集群
			// try each server for the shard.
			for si := 0; si < len(servers); si++ { // 遍历对应的raft集群，轮寻发送请求
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond) // 休眠10ms
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1) // 更新成最新的版本控制信息
	}

	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.seqId++ // 更新请求序列号
	for {
		args := PutAppendArgs{ // 生成请求参数
			Key:      key,
			Value:    value,
			Op:       op,
			Clientid: ck.clientId,
			SeqId:    ck.seqId,
		}
		shard := key2shard(key)                       // 获取对应的分片/分区的Id号
		gid := ck.config.Shards[shard]                // 获取对应分片/分区所在的raft集群Id
		if servers, ok := ck.config.Groups[gid]; ok { // 获取对应的raft集群
			for si := 0; si < len(servers); si++ { // 遍历raft集群，轮寻发送请求
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok && reply.Err == OK {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
