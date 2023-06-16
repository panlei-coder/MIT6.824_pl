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
	ck.servers = servers // 分片控制器raft集群
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
	args := QueryArgs{Num: num, ClientId: ck.clientId, SeqId: ck.seqId}

	// 2.记录当前请求的服务端的id号，当请求失败时，需要根据之前请求的id号以轮寻的方式依次向服务端发送请求
	currentServerId := ck.leaderId

	// 3.循环的向服务端发送请求，一旦发生了超时或者请求失败则向下一个服务端发送请求
	for {
		// try each known server.
		// 3.1.生成请求结果参数，并向服务端currentServerId发送query请求
		reply := QueryReply{}
		ok := ck.servers[currentServerId].Call("ShardCtrler.Query", &args, &reply)

		// 3.2.判断请求是否成功，如果成功了就直接返回，否则失败了则换一个服务器重新发送请求
		if ok {
			if reply.Err == OK { // 请求成功
				ck.leaderId = currentServerId
				return reply.Config
			} else if reply.Err == ErrWrongLeader { // 请求失败了
				currentServerId = (currentServerId + 1) % len(ck.servers)
				continue
			}
		}

		/*for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}*/

		// 3.3.针对每次请求都设置了超时机制，如果超时未响应(节点可能发生了crash等情况)，则直接换一个服务器重新发送
		currentServerId = (currentServerId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	// Your code here.
	// 1.更新客户端请求序列seqId，生成join的请求参数
	ck.seqId++
	args := JoinArgs{Servers: servers, SeqId: ck.seqId, ClientId: ck.clientId}

	// 2.记录向当前服务端发送请求的id，便于后续进行轮寻向服务端发送请求
	currentServerId := ck.leaderId

	// 3.循环的向服务端发送请求，一旦发生了超时或者请求失败则向下一个服务端发送请求
	for {
		// try each known server.
		// 3.1.生成向服务端发送请求的参数，并向服务端发送请求
		reply := JoinReply{}
		ok := ck.servers[currentServerId].Call("ShardCtrler.Join", &args, &reply)

		// 3.2.根据向服务端发送请求之后的响应结果判断，如果响应成功，则返回结果，否则失败直接换一个服务端重新发送请求
		if ok {
			if reply.Err == OK { // 请求成功
				ck.leaderId = currentServerId
				return
			}
		} else if reply.Err == ErrWrongLeader { // 请求失败
			currentServerId = (currentServerId + 1) % len(ck.servers)
			continue
		}

		/*for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}*/

		// 3.3.针对每一次向服务端发送请求时都设置超时机制，如果超时了直接换一个服务端进行请求
		currentServerId = (currentServerId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	// Your code here.
	// 1.更新客户端请求序列，并生成向服务器端发送leave请求的参数
	ck.seqId++
	args := LeaveArgs{GIDs: gids, SeqId: ck.seqId, ClientId: ck.clientId}

	// 2.记录向当前服务端发送请求的id，便于后续进行轮寻向服务端发送请求
	currentServerId := ck.leaderId

	// 3.循环的向服务端发送请求，一旦发生了超时或者请求失败则向下一个服务端发送请求
	for {
		// try each known server.
		// 3.1.生成向服务端发送请求的参数，并向服务端发送请求
		reply := LeaveReply{}
		ok := ck.servers[currentServerId].Call("ShardCtrler.Leave", &args, &reply)

		// 3.2.根据向服务端发送请求之后的响应结果判断，如果响应成功，则返回结果，否则失败直接换一个服务端重新发送请求
		if ok {
			if reply.Err == OK { // 请求成功
				ck.leaderId = currentServerId
				return
			}
		} else if reply.Err == ErrWrongLeader { // 请求失败
			currentServerId = (currentServerId + 1) % len(ck.servers)
			continue
		}

		/*for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}*/

		// 3.3.针对每一次向服务端发送请求时都设置超时机制，如果超时了直接换一个服务端进行请求
		currentServerId = (currentServerId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	// Your code here.
	// 1.更新客户端请求序列，并生成向服务器端发送leave请求的参数
	ck.seqId++
	args := MoveArgs{Shard: shard, GID: gid, SeqId: ck.seqId, ClientId: ck.clientId}

	// 2.记录向当前服务端发送请求的id，便于后续进行轮寻向服务端发送请求
	currentServerId := ck.leaderId

	// 3.循环的向服务端发送请求，一旦发生了超时或者请求失败则向下一个服务端发送请求
	for {
		// try each known server.
		// 3.1.生成向服务端发送请求的参数，并向服务端发送请求
		reply := MoveReply{}
		ok := ck.servers[currentServerId].Call("ShardCtrler.Move", &args, &reply)

		// 3.2.根据向服务端发送请求之后的响应结果判断，如果响应成功，则返回结果，否则失败直接换一个服务端重新发送请求
		if ok {
			if reply.Err == OK { // 请求成功
				ck.leaderId = currentServerId
				return
			}
		} else if reply.Err == ErrWrongLeader { // 请求失败
			currentServerId = (currentServerId + 1) % len(ck.servers)
			continue
		}

		/*for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}*/

		// 3.3.针对每一次向服务端发送请求时都设置超时机制，如果超时了直接换一个服务端进行请求
		currentServerId = (currentServerId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
