package kvraft

import (
	"6.824/labrpc"
	"fmt"
	"os"
)
import "crypto/rand"
import "math/big"
import mathrand "math/rand"

type Clerk struct {
	servers []*labrpc.ClientEnd // 客户端能够访问的多个服务器
	// You will have to modify this struct.

	seqId    int   // 向服务器发送请求时的序列号，避免服务器在处理完客户端请求之后crash,未来得及向客户端reponse,使得客户端再次请求时被再次执行
	leaderId int   // 记录服务器集群中的leaderId，便于在下一次进行服务器访问时不用再进行查询（如果leaderId没有变的话）
	clientId int64 // 客户端自己的编号
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// -------------------------------------------------初始化（Make）部分-----------------------------------------

// 创建一个客户端
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.

	//ck.seqId = 0
	ck.clientId = nrand()
	ck.leaderId = mathrand.Intn(len(ck.servers)) // 利用库函数随机生成leaderId

	return ck
}

// -------------------------------------------------RPC部分-----------------------------------------
//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
// 根据key获取对应的value，如果不存在的key，返回“”
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.seqId += 1
	args := GetArgs{
		Key:      key,
		CliendId: ck.clientId,
		SeqId:    ck.seqId,
	}

	serverId := ck.leaderId // 这个未必是真的服务器leaderId
	for {
		reply := GetReply{}
		//ck.printClientGet(args, serverId)
		//fmt.Printf("[++++Client[%v]++++]:send a Get,args:%+v,serverId[%v]\n", ck.clientId, args, serverId)
		// 客户端调用相应的服务器函数
		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)

		if ok { // 如果向服务器请求得到响应
			if reply.Err == ErrNoKey { // 如果没有找到相关的key，则直接返回“”
				ck.leaderId = serverId // 保存服务器leaderId
				return ""
			} else if reply.Err == ErrWrongLeader { // 如果返回的错误是leaderId不正确，则更新serverId，重新发送请求
				serverId = (serverId + 1) % len(ck.servers) // 更新服务器leaderId
				continue
			} else if reply.Err == OK {
				ck.leaderId = serverId // 保存服务器leaderId
				return reply.Value
			}
		} else { // 如果向服务器请求没有得到响应，则更新服务器leaderId（可能服务器出现了crash等原因，导致请求失败了）
			serverId = (serverId + 1) % len(ck.servers)
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
// op理解为opType更合适
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seqId += 1
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		SeqId:    ck.seqId,
	}

	serverId := ck.leaderId
	for {
		reply := PutAppendReply{}
		//ck.printClientPutAppend(op, args, serverId)
		//fmt.Printf("[++++Client[%v]++++]:send a %v,args:%+v,serverId[%v]\n", ck.clientId, op, args, serverId)
		// 客户端调用相应的服务器函数
		ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)

		if ok {
			if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			} else if reply.Err == OK {
				ck.leaderId = serverId
				return
			}
		} else {
			serverId = (serverId + 1) % len(ck.servers)
		}
	}

}

// 替换数据库中特定键的值
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// 将arg附加到键的值
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// -------------------------------------------------utils部分-----------------------------------------
// 将client的Get信息输出
func (ck *Clerk) printClientGet(args GetArgs, serverId int) error {
	result := "Client_" + fmt.Sprintf("%d", ck.clientId) + ".txt"
	// 打开文件，如果文件不存在就创建
	file, err := os.OpenFile(result, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := fmt.Fprintf(file, "[++++Client[%v]++++]:send a Get(opType),args:%+v,serverId[%v]\n", ck.clientId, args, serverId); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(file, "\n"); err != nil {
		return err
	}

	return nil
}

// 将client的PutAppend信息输出
func (ck *Clerk) printClientPutAppend(opType string, args PutAppendArgs, serverId int) error {
	result := "Client_" + fmt.Sprintf("%d", ck.clientId) + ".txt"
	// 打开文件，如果文件不存在就创建
	file, err := os.OpenFile(result, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := fmt.Fprintf(file, "[++++Client[%v]++++]:send a %v(opType),args:%+v,serverId[%v]\n", ck.clientId, opType, args, serverId); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(file, "\n"); err != nil {
		return err
	}

	return nil
}
