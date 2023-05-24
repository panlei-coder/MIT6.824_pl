package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	mathrand "math/rand"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.

	seqId    int   // 客户端请求的序列号
	leaderId int   // 确定哪个是raft集群中的leader，下次再进行请求时直接发送给该服务器
	clientId int64 // 客户端的id号
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
	// Your code here.

	// ck.seqId = 0 // 会被默认初始化为0
	ck.clientId = nrand()                        // 随机初始化一个客户端编号
	ck.leaderId = mathrand.Intn(len(ck.servers)) // 随机生成一个[0,len(ck.servers))之间的leaderId

	return ck
}

func (ck *Clerk) Query(num int) Config {
	// Your code here.

	// 1.更新客户端请求的序列号，并生成query请求的参数
	ck.seqId++
	args := &QueryArgs{Num: num, ClientId: ck.clientId, SeqId: ck.seqId}

	// 2.记录当前请求的服务端的id号，当请求失败时，需要根据之前请求的id号以轮寻的方式依次向服务端发送请求
	currentServerId := ck.leaderId

	// 3.循环的向服务端发送请求，一旦发生了超时或者请求失败则向下一个服务端发送请求
	for {
		// try each known server.
		// 3.1.生成请求结果参数，并向服务端currentServerId发送请求
		reply := QueryReply{}
		ok := ck.servers[currentServerId].Call("ShardCtrler.Query", &args, &reply)
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
