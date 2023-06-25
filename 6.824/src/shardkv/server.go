package shardkv

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"

//----------------------------------------------------结构体定义部分------------------------------------------------------
const (
	UpConfigLoopInterval = 100 * time.Millisecond // 更新配置的间隔时间
	GetTimeout           = 500 * time.Millisecond // Get操作的超时时间
	AppOrPutTimeout      = 500 * time.Millisecond // Append/Put操作的超时时间
	UpConfigTimeout      = 500 * time.Millisecond // 更新配置的超时时间
	AddShardsTimeout     = 500 * time.Millisecond // 添加分片/分区的超时时间（分片/分区的迁移）
	RemoveShardsTimeout  = 500 * time.Millisecond // 删除分片/分区的超时时间（分片/分区的迁移）
)

type Shard struct {
	KvMap     map[string]string // kv存储
	ConfigNum int               // 配置新的版本号（进行操作的序列号）
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64              // 客户端Id
	SeqId    int                // 序列号
	OpType   string             // 操作的类型
	Key      string             // put/append/get需要用的key
	Value    string             // put/append需要的value
	UpConfig shardctrler.Config // 更新的版本配置信息
	ShardId  int                // 分片/分区在的Id
	Shard    Shard              // 分片/分区的内容
	SeqMap   map[int64]int      // 客户端的请求序列号（todo 这里为什么需要所有客户端的请求序列号？）
}

type OpReply struct {
	ClientId int64 // 客户端的Id
	SeqId    int   // 请求序列号
	Err      Err   // 错误码
}

type ShardKV struct {
	mu           sync.Mutex                     // 互斥锁
	me           int                            // 服务端的Id
	rf           *raft.Raft                     // raft分布式共识算法
	applyCh      chan raft.ApplyMsg             // ApplyMsg的channel
	make_end     func(string) *labrpc.ClientEnd // 获取服务端的接口
	gid          int                            // 当前ShardKV所属的groupId
	ctrlers      []*labrpc.ClientEnd            // 分片控制器的raft集群
	maxraftstate int                            // snapshot if log grows this big 最大的日志上限，超过需要生成日志快照（持久化）

	// Your definitions here.
	dead int32 // 标记当前ShardKV是否被kill

	lastIncludeIndex int // 记录日志快照生成的位置

	Config     shardctrler.Config // 需要更新的最新配置（持久化）
	LastConfig shardctrler.Config // 更新之前的最新配置，需要进行对比，判断是否全部都更新完了（持久化）

	shardsPersist []Shard              // ShardId -> Shard 如果KvMap == nil则说明当前的数据不归当前分片管（持久化）
	waitChMap     map[int]chan OpReply // 将来自不同的客户端的请求回复
	SeqMap        map[int64]int        // 不同客户端的请求序列（持久化）
	sck           *shardctrler.Clerk   // sck is a client used to contact shard master 作为客户端连接分片控制器
}

//----------------------------------------------------初始化(Start)部分------------------------------------------------------
//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{}) // 做Op的检查与注册，需要存放的内容可能会被持久化

	kv := new(ShardKV)             // 创建一个ShardKV
	kv.me = me                     // 一个当前服务器Id
	kv.maxraftstate = maxraftstate // 需要生成日志快照的最大上限
	kv.make_end = make_end         //
	kv.gid = gid                   // group的gid
	kv.ctrlers = ctrlers           // 分片控制器

	// Your initialization code here.

	kv.shardsPersist = make([]Shard, shardctrler.NShards) // 当前raft集群中所有分片/分区对应内容的映射
	kv.SeqMap = make(map[int64]int)                       // 来自客户端的请求序列

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.sck = shardctrler.MakeClerk(kv.ctrlers) // 因为服务端也需要同分片控制器进行通信，所以服务端需要生成一个作为分片控制器的客户端
	kv.waitChMap = make(map[int]chan OpReply)  // server用于记录同下一层的kv存储层通信的channel

	kv.lastIncludeIndex = 0

	// 在启动客户端时，需要读取日志快照，如果有则进行解码应用
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapShot(snapshot)
	}

	kv.applyCh = make(chan raft.ApplyMsg)                 // 用于存放下一层的kv存储层传上来需要应用的ApplyMsg
	kv.rf = raft.Make(servers, me, persister, kv.applyCh) // 生成raft分布式共识算法的接口

	go kv.applyMsgHandlerLoop() // 开启消息处理协程
	go kv.configDetectedLoop()  // 开启版本信息的检测协程

	return kv
}

//------------------------------------------------------RPC部分--------------------------------------------------------

// get操作
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// 1.先判断传进来的Key所在分区是否在当前Group;如果是在当前的Group，但是对应的kvMap==nil，说明发生了分区迁移，只是Shard还没有达到当前Group
	shardId := key2shard(args.Key) // 分区/分片在数组Shards的下标
	kv.mu.Lock()
	if kv.Config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardsPersist[shardId].KvMap == nil {
		reply.Err = ShardNotArrived
	}
	kv.mu.Unlock()

	// 2.根据检查的结果，进行处理（这样写是为了释放锁）
	if reply.Err == ErrWrongGroup || reply.Err == ShardNotArrived {
		return
	}

	// 3.生成Get对应的Op
	command := Op{
		OpType:   GetType,
		ClientId: args.Clientid,
		SeqId:    args.SeqId,
		Key:      args.Key,
	}

	// 4.调用startCommand接口将command添加到底层的raft中
	err := kv.startCommand(command, GetTimeout)
	if err != OK { // 说明命令添加失败
		reply.Err = err
		return
	}

	// 5.命令添加成功的话，需要将对应的value返回（必须要检查当前的shard是否发生了分片/分区迁移）
	kv.mu.Lock()
	if kv.Config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardsPersist[shardId].KvMap == nil {
		reply.Err = ShardNotArrived
	} else {
		reply.Err = OK
		reply.Value = kv.shardsPersist[shardId].KvMap[args.Key]
	}
	kv.mu.Unlock()
	return
}

// put操作
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// 1.先判断传进来的Key所在分区是否在当前Group;如果是在当前的Group，但是对应的kvMap==nil，说明发生了分区迁移，只是Shard还没有达到当前Group
	shardId := key2shard(args.Key) // 分区/分片在数组Shards的下标
	kv.mu.Lock()
	if kv.Config.Shards[shardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardsPersist[shardId].KvMap == nil {
		reply.Err = ShardNotArrived
	}
	kv.mu.Unlock()

	// 2.根据检查的结果，进行处理（这样写是为了释放锁）
	if reply.Err == ErrWrongGroup || reply.Err == ShardNotArrived {
		return
	}

	// 3.生成put/append对应的Op
	command := Op{
		OpType:   args.Op,
		ClientId: args.Clientid,
		SeqId:    args.SeqId,
		Key:      args.Key,
		Value:    args.Value,
	}

	// 4.调用startCommand接口将command添加到底层的raft中
	reply.Err = kv.startCommand(command, AppOrPutTimeout)
	return
}

// 添加迁移过来的shard
func (kv *ShardKV) AddShard(args *SendShardArg, reply *AddShardReply) {
	// todo ? 是否需要考虑判断当前添加的这个被迁移过来的shard是否属于当前的Group
	// 1.先判断传进来的Key所在分区是否在当前Group;如果是在当前的Group
	//(kvMap = nil，因为发生了分区迁移，Shard还没有达到当前Group，所以kvMap一定为nil）
	kv.mu.Lock()
	if kv.Config.Shards[args.ShardId] != kv.gid {
		reply.Err = ErrWrongGroup
	} else if kv.shardsPersist[args.ShardId].KvMap != nil {
		reply.Err = ErrInconsistentData
	}
	kv.mu.Unlock()

	// 2.根据检查的结果，进行处理（这样写是为了释放锁）
	if reply.Err == ErrWrongGroup {
		return
	}

	// 3.生成addShard对应的Op
	command := Op{
		OpType:   AddShardType,
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		ShardId:  args.ShardId,
		Shard:    args.Shard,
		SeqMap:   args.LastAppliedSeqId,
	}

	// 4.调用startCommand接口将command添加到底层的raft中
	reply.Err = kv.startCommand(command, AddShardsTimeout)
	return
}

//------------------------------------------------------handler部分--------------------------------------------------------

// 更新处理最新的配置信息
func (kv *ShardKV) upConfigHandler(op Op) {
	// 1.比较当前的版本配置与更新后的版本配置之间是否满足更新要求
	curConfig := kv.Config
	upConfig := op.UpConfig
	if curConfig.Num >= upConfig.Num { // 不满足，不需要更新
		return
	}

	// 2.满足更新要求，进行版本配置更新
	for shard, gid := range upConfig.Shards {
		// 如果更新后的配置中分片正好属于当前group，且更新之前的配置中该分片没有被分配存储空间
		//（todo ？ 被迁移走的Shard对应的存储空间在remove做相应的处理）
		if gid == kv.gid && curConfig.Shards[shard] == shardctrler.InvalidGid {
			kv.shardsPersist[shard].KvMap = make(map[string]string)
			kv.shardsPersist[shard].ConfigNum = upConfig.Num
		}
	}

	kv.LastConfig = curConfig
	kv.Config = upConfig
}

// 添加迁移的分片/分区
func (kv *ShardKV) addSharHandler(op Op) {
	// 1.判断需要添加的shard对应的位置是否已经存在了或者添加的shard对应的configNum是否满足要求（todo ? op.Shard.ConfigNum != kv.Config.Num）
	if kv.shardsPersist[op.ShardId].KvMap != nil || op.Shard.ConfigNum < kv.Config.Num {
		return
	}

	// 2.添加shard
	kv.shardsPersist[op.ShardId] = kv.cloneShard(op.Shard.ConfigNum, op.Shard.KvMap)

	// 3.shard原本所在的server所对应的SeqMap需要同步到当前的server中
	for clientId, seqId := range op.SeqMap {
		// 如果当前server没有的请求序列或者是比当前server所拥有的序列号要新，那么就需要进行更新
		if r, ok := kv.SeqMap[clientId]; !ok || r < seqId {
			kv.SeqMap[clientId] = seqId
		}
	}
}

// 删除迁移的分片/分区（更确切的说应该只需要删除当前最新配置下不再负责的shard，以前版本没有被删除的可以继续保留，不受影响）
func (kv *ShardKV) removeShardHandler(op Op) {
	// 需要删除当前版本号下的shard，以前的就不需要管(可以保存自己不再需要分片，因此主要是删除当前版本配置中多余的分片)
	if op.SeqId < kv.Config.Num {
		return
	}
	kv.shardsPersist[op.ShardId].KvMap = nil
	kv.shardsPersist[op.ShardId].ConfigNum = op.SeqId // 这里的SeqId为最新配置的版本号
}

//------------------------------------------------------Loop部分--------------------------------------------------------

// 处理发送过来的ApplyMsg（put/append/get/addshard/removeshard/upconfig）
func (kv *ShardKV) applyMsgHandlerLoop() {
	for {
		// 1.检查当前的server是否被kill
		if kv.Killed() {
			return
		}

		// 2.通过select模型处理底层raft传上来的applyMsg
		select {
		case msg := <-kv.applyCh:
			// 2.1.如果是command类型的applyMsg
			if msg.CommandValid {
				// 2.1.1.如果对应的command已经生成了日志快照，则直接跳过
				if msg.CommandIndex <= kv.lastIncludeIndex {
					return
				}

				// 2.1.1.生成对应的reply
				kv.mu.Lock()
				op := msg.Command.(Op) // 判断是否是Op类型的数据，如果是就转换成Op类型，不是抛出错误
				reply := OpReply{
					ClientId: op.ClientId,
					SeqId:    op.SeqId,
					Err:      OK,
				}

				// 2.1.2.如果op是put/append/get操作
				if op.OpType == PutType || op.OpType == AppendType || op.OpType == GetType {
					// 一旦操作shard都需要判断是否还是当前的group以及shard是否到达了当前的group
					shardId := key2shard(op.Key)
					if kv.Config.Shards[shardId] != kv.gid {
						reply.Err = ErrWrongGroup
					} else if kv.shardsPersist[shardId].KvMap == nil {
						reply.Err = ShardNotArrived
					} else {
						if !kv.ifDuplicate(op.ClientId, op.SeqId) { // 如果不是重复请求序列
							// 更新对应客户端的请求序列
							kv.SeqMap[op.ClientId] = op.SeqId
							switch op.OpType {
							case PutType:
								kv.shardsPersist[shardId].KvMap[op.Key] = op.Value
							case AppendType:
								kv.shardsPersist[shardId].KvMap[op.Key] += op.Value
							case GetType:
								// 如果是get这里不需要做处理
							default:
								log.Fatalf("invalid command type:%v.", op.OpType)
							}
						}
					}
				} else if op.OpType == AddShardType || op.OpType == RemoveShardType || op.OpType == UpConfigType {
					switch op.OpType {
					case AddShardType:
						// 因为对于分片/分区迁移时，op中的SeqId为对应的配置版本号，如果如果配置的版本号比op的SeqId还要低，说明版本配置没有更新
						if kv.Config.Num < op.SeqId { // 小于说明，新的版本配置信息还没有到，大于等于才可以添加相应的shard
							reply.Err = ConfigNotArrived
							break
						}

						kv.addSharHandler(op)
					case RemoveShardType:
						// 小于说明，新的版本配置信息还没有到（大于其实在removeShardHandler函数处理时页忽略掉了，只有等于这一种情况需要处理）
						if kv.Config.Num < op.SeqId {
							reply.Err = ConfigNotArrived
							break
						}
						kv.removeShardHandler(op)
					case UpConfigType:
						kv.upConfigHandler(op)
					default:
						log.Fatalf("invalid command type:%v.", op.OpType)
					}
				}

				// 2.1.3.这里applyMsg处理完成之后判断是否需要生成快照(maxraftstate != -1说明需要生成快照)
				if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
					snapshot := kv.PersistSnapShot()           // 生成日志快照
					kv.rf.Snapshot(msg.CommandIndex, snapshot) // 持久化日志快照
				}

				kv.mu.Unlock()

				// 因为msg.CommandIndex与ch是一一对应关系
				ch := kv.getWaitCh(msg.CommandIndex)
				ch <- reply
			}

			// 2.2.如果是snapshot类型的applyMsg
			if msg.SnapshotValid {
				kv.mu.Lock()
				if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
					// 读取快照数据
					kv.DecodeSnapShot(msg.Snapshot)
					kv.lastIncludeIndex = msg.SnapshotIndex
				}

				kv.mu.Unlock()
			}
		}
	}
}

// 配置检测（将版本配置更新之后发生的分片/分区迁移进行相应的处理，如果相应的分片/分区迁移没有完成，
// 那么当前的servers是不可以进入下一个新的配置，直到所有的分片/分区迁移工作都完成之后，才允许更新成下一个新的配置）
func (kv *ShardKV) configDetectedLoop() {
	// 1.获取当前配置信息和raft
	kv.mu.Lock()
	rf := kv.rf
	sck := kv.sck
	kv.mu.Unlock()

	// 2.循环检测配置
	for {
		// 2.1.如果当前server被kill，则直接结束
		if kv.Killed() {
			return
		}

		// 2.2.判断当前server是否为leader
		if _, isLeader := rf.GetState(); !isLeader {
			time.Sleep(UpConfigLoopInterval)
			continue
		}

		// 2.3.判断是否把不属于自己的部分全部发送给了其他服务器
		kv.mu.Lock()
		curConfig := kv.Config
		if !kv.allSent() {
			// 2.3.1.复制一份请求序列号（用于分片/分区迁移之后相应的servers更新对应的客户端请求序列号）
			SeqMap := make(map[int64]int)
			for k, v := range kv.SeqMap {
				SeqMap[k] = v
			}

			// 2.3.2.将最新配置中不属于当前group的分片分给别人
			for shardId, gid := range kv.LastConfig.Shards {
				if gid == kv.gid && kv.Config.Shards[shardId] != kv.gid &&
					kv.shardsPersist[shardId].ConfigNum < kv.Config.Num {
					// 深拷贝一个出来，将复制出来的shard发送给新配置下的servers
					sendData := kv.cloneShard(kv.Config.Num, kv.shardsPersist[shardId].KvMap)
					args := SendShardArg{
						LastAppliedSeqId: SeqMap,
						ShardId:          shardId,
						Shard:            sendData,
						ClientId:         int64(gid),
						SeqId:            kv.Config.Num,
					}

					// 根据新配置的信息，获取到对应的servers
					serversList := kv.Config.Groups[kv.Config.Shards[shardId]]
					servers := make([]*labrpc.ClientEnd, len(serversList))
					for i, name := range serversList {
						servers[i] = kv.make_end(name)
					}

					kv.mu.Unlock()

					// 开启协程对每个客户端发送切片（这里需要注意死锁，开启了协程，所以需要将锁释放后续再重新获取）
					go func(servers []*labrpc.ClientEnd, args *SendShardArg) {
						num := len(servers)
						serverId := 0
						start := time.Now()
						for {
							reply := AddShardReply{}
							//ck.printClientGet(args, serverId)
							//fmt.Printf("[++++Client[%v]++++]:send a Get,args:%+v,serverId[%v]\n", ck.clientId, args, serverId)
							// 客户端调用相应的服务器函数
							ok := servers[serverId].Call("ShardKV.AddShard", args, &reply)

							// 如果返回了OK或者是超时了就进行GC操作，删除掉不属于自己的shard
							//（todo ？ 这个超时感觉不合理，超时没有将shard发送给对应的servers就将shard从当前的servers删除掉，不会造成shard丢失么？）
							// 如果分片在一定的时间内没有转移成功，就认为当前这个分片被丢失掉了，所以这里设置了超时机制，即超过设定的时间，还未收到分片转移成功的通知，就认为该分片丢失了s
							if (ok && reply.Err == OK) || time.Now().Sub(start) > 2*time.Second { // || time.Now().Sub(start) > 2*time.Second
								kv.mu.Lock()
								op := Op{
									OpType:   RemoveShardType,
									ClientId: int64(kv.gid),
									SeqId:    kv.Config.Num,
									ShardId:  args.ShardId,
								}
								kv.mu.Unlock()

								kv.startCommand(op, RemoveShardsTimeout)
								break // 发送成功了之后就直接退出，剩下的全靠raft集群自身的分布式共识算法进行同步
							} else { // 如果向服务器请求没有得到响应，则更新服务器leaderId（可能服务器出现了crash等原因，导致请求失败了）
								serverId = (serverId + 1) % num
								if serverId == 0 { // 将所有的server都测试了一边，都没有成功，说明可能存在crash等其他原因导致无法进行raft同步，则sleep相应的时间
									time.Sleep(UpConfigLoopInterval)
								}
							}
						}
					}(servers, &args)

					kv.mu.Lock()
				}
			}

			// 因为存在某些shard还没有发送完，所以sleep一段时间，保证将没有发送完的部分发送出去
			kv.mu.Unlock()
			time.Sleep(UpConfigLoopInterval)
			continue
		}

		// 2.4.判断是否全部接收到了来自其他服务器迁移的shard
		if !kv.allReceived() {
			// 因为存在某些shard还没有接收到，所以sleep一段时间，保证将全部都接收到
			kv.mu.Unlock()
			time.Sleep(UpConfigLoopInterval)
			continue
		}
		kv.mu.Unlock()

		// 2.5.查询最新版本配置信息，如果当前的版本配置信息与查询的一致，则不需要更新配置信息，否则需要更新配置信息
		NewConfig := sck.Query(curConfig.Num + 1)
		if NewConfig.Num != (curConfig.Num + 1) {
			time.Sleep(UpConfigLoopInterval)
			continue
		}

		// 2.6.有了更新的版本配置信息，所以需要更新
		op := Op{
			OpType:   UpConfigType,
			ClientId: int64(kv.gid),
			SeqId:    NewConfig.Num,
			UpConfig: NewConfig,
		}
		kv.startCommand(op, UpConfigTimeout)
	}
}

//------------------------------------------------------持久化快照部分--------------------------------------------------------

// 生成日志快照
func (kv *ShardKV) PersistSnapShot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.shardsPersist)
	e.Encode(kv.SeqMap)
	e.Encode(kv.maxraftstate)
	e.Encode(kv.Config)
	e.Encode(kv.LastConfig)
	data := w.Bytes()
	//fmt.Printf("RaftNode[%d] persist starts, currentTerm[%d] voteFor[%d] log[%v]\n", rf.me, rf.currentTerm, rf.votedFor, rf.logs)
	return data
}

// 解码日志快照
func (kv *ShardKV) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any status?
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var shardsPersitst []Shard
	var SeqMap map[int64]int
	var maxraftstate int
	var Config, LastConfig shardctrler.Config
	if d.Decode(&shardsPersitst) != nil ||
		d.Decode(&SeqMap) != nil ||
		d.Decode(&maxraftstate) != nil ||
		d.Decode(&Config) != nil ||
		d.Decode(&LastConfig) != nil {
		fmt.Println("decode error")
	} else {
		kv.shardsPersist = shardsPersitst
		kv.SeqMap = SeqMap
		kv.maxraftstate = maxraftstate
		kv.Config = Config
		kv.LastConfig = LastConfig
	}
}

//------------------------------------------------------utils封装部分----------------------------------------------------

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//

// kill掉当前的server
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1) // 对dead进行原子操作，用于标记当前server是否被kill
	kv.rf.Kill()                   // 将底层的raft也kill
	// Your code here, if desired.
}

// 判断当前server是否被kill
func (kv *ShardKV) Killed() bool {
	z := atomic.LoadInt32(&kv.dead) // 原子的加载
	return z == 1
}

// 判断是否存在重复的请求序列，避免多次进行处理
func (kv *ShardKV) ifDuplicate(clientId int64, seqId int) bool {
	lastSeqId, exist := kv.SeqMap[clientId]
	if !exist { // 不存在直接返回false
		return false
	}
	return seqId <= lastSeqId
}

// 通过index获取对应的channel，如果没有则创建一个返回回去
func (kv *ShardKV) getWaitCh(index int) chan OpReply {
	// 1.获取锁
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 2.获取index对应的channel，如果不存在则直接创建一个
	ch, exist := kv.waitChMap[index]
	if !exist {
		kv.waitChMap[index] = make(chan OpReply, 1)
		ch = kv.waitChMap[index]
	}

	return ch
}

// 判断是否将需要迁移的shard全部发送给对应的服务器
func (kv *ShardKV) allSent() bool {
	for shard, gid := range kv.LastConfig.Shards {
		// todo ？ kv.shardsPersist[shard].ConfigNum < kv.Config.Num
		// 如果当前配置中分片中的信息不匹配，且持久化中的配置版本号更小，说明还没有发送
		if gid == kv.gid && kv.Config.Shards[shard] != kv.gid &&
			kv.shardsPersist[shard].ConfigNum < kv.Config.Num {
			return false
		}
	}

	return true
}

func (kv *ShardKV) allReceived() bool {
	for shard, gid := range kv.LastConfig.Shards {
		// todo ？ kv.shardsPersist[shard].ConfigNum < kv.Config.Num
		// 如果当前配置中分片中的信息匹配，但之前的信息不匹配，且持久化中的配置版本号更小，说明还没有接收到了
		if gid != kv.gid && kv.Config.Shards[shard] == kv.gid &&
			kv.shardsPersist[shard].ConfigNum < kv.Config.Num {
			return false
		}
	}
	return true
}

// 统一处理get/put/append这些操作命令，添加日志，并通过下层的raft共识算法实现raft集群的同步
func (kv *ShardKV) startCommand(command Op, timeoutPeriod time.Duration) Err {
	// 1.判断添加的时候是否找对了leader
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader { // 寻找错了leader
		kv.mu.Unlock()
		return ErrWrongLeader
	}
	kv.mu.Unlock()

	// 2.通过index获取对应的channel
	ch := kv.getWaitCh(index)
	defer func() { // 对应的channel使用完了之后直接删除掉
		kv.mu.Lock()
		delete(kv.waitChMap, index)
		kv.mu.Unlock()
	}()

	// 3.创建一个计时器，到达指定时间后就向通道中发送时间事件，如果没有响应就做超时处理
	timer := time.NewTicker(timeoutPeriod)
	defer timer.Stop() // 函数结束之前停止计时

	// 4.通过select模型来判断当前的channel是什么类型的消息
	select {
	case reply := <-ch: // applyMsgHandlerLoop会将处理的结果，添加到通道中
		kv.mu.Lock()
		if reply.SeqId != command.SeqId || reply.ClientId != command.ClientId { // 如果对应通道获取到的数据和之前传进去的op信息不对应，说明与底层的raft存在数据不一致的情况
			kv.mu.Unlock()
			return ErrInconsistentData
		}
		kv.mu.Unlock()

		return reply.Err

	case <-timer.C: // 如果在规定的时间内没有响应，则超时处理
		return ErrOverTime
	}
}

// clone一个shard todo ? 复制一个的目的
func (kv *ShardKV) cloneShard(ConfigNum int, KvMap map[string]string) Shard {
	migrateShard := Shard{
		KvMap:     make(map[string]string),
		ConfigNum: ConfigNum,
	}
	for k, v := range KvMap {
		migrateShard.KvMap[k] = v
	}

	return migrateShard
}
