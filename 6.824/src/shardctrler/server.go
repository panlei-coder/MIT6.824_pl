package shardctrler

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"os"
	"time"
)
import "sync"

const (
	JoinType  = "join"
	LeaveType = "leave"
	MoveType  = "move"
	QueryType = "query"

	JoinOverTime  = 100
	LeaveOverTime = 100
	MoveOverTime  = 100
	QueryOverTime = 100

	InvalidGid = 0
)

// ------------------------------------------------------------------结构体定义部分-----------------------------------------------------

type ShardCtrler struct {
	mu      sync.Mutex         // 互斥锁，保护共享资源
	me      int                // 服务端的id号
	rf      *raft.Raft         // raft共识算法的接口
	applyCh chan raft.ApplyMsg // raft共识算法的消息channel

	// Your data here.
	seqMap    map[int64]int   // 记录当前客户端发给服务端的最新请求序列号，用于重新性检测，避免重复处理同一个请求
	waitChMap map[int]chan Op // 传递由下层raft服务的appCh传过来的command，index/chan(Op)

	configs []Config // indexed by config num,存放的是一系列版本的配置信息，最新的下标对应的最新的配置信息
}

type Op struct {
	// Your data here.
	OpType   string
	ClientId int64
	SeqId    int

	// query
	QueryNum int // 查询的版本号

	// join
	JoinServers map[int][]string // join的服务器信息

	// leave
	LeaveGids []int // 需要移除的服务器组的信息

	// move
	MoveShard int // 需要分配的分片Id
	MoveGid   int // 分配到指定组的组Id
}

// ------------------------------------------------------------------初始化(Start)部分-----------------------------------------------------

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	// 1.创建一个分片控制器的server，并分配服务器Id
	sc := new(ShardCtrler)
	sc.me = me

	// 2.初始化configs，作为dummy config，起到哨兵作用
	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	// 3.注册传递的操作类型，并初始化消息chanel和raft分布式共识算法的接口
	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	// 4.初始化客户端的最新请求序列组和等待应用的信息
	sc.seqMap = make(map[int64]int)
	sc.waitChMap = make(map[int]chan Op)

	// 5.开启一个协程单独处理join，move，leave这三个请求，而query查询完了之后可以直接返回，不需要单独处理
	go sc.applyMsgHandlerLoop()

	return sc
}

// ------------------------------------------------------------------RPC部分-----------------------------------------------------

// join参数就是为一个组，一个组对应的就是gid->lists of server names
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.

	// 1.获取当前server的状态，判断是不是分片控制器集群中的leader，
	// 如果不是leader，说明client发错了节点，返回一个错误码给client，client重发
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 2.将Op封装并传到下一层，添加到日志中，并利用raft分布式共识算法进行同步
	op := Op{OpType: JoinType, SeqId: args.SeqId, ClientId: args.ClientId, JoinServers: args.Servers}
	lastIndex, _, _ := sc.rf.Start(op)

	// 3.获取由下层raft通过chan传过来的信息，这里主要是为lastIndex对应的位置分配存储空间，并利用下面select模型监听ch通道
	ch := sc.getWaitCh(lastIndex)
	defer func() { // 处理完了之后需要将make出来的空间根据key来释放掉
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex)
		sc.mu.Unlock()
	}()

	// 4.设置超时时间（创建了一个定时器，每间隔设定的时间便会向channel发送一个时间事件）
	timer := time.NewTicker(JoinOverTime * time.Millisecond)
	defer timer.Stop() // 这里推迟stop直到下面的<-timer.C发生为止（退出当前函数）

	// 5.利用select进行通信，获取chan中传过来的replyOp，进行判断是否Get成功
	select {
	case replyOp := <-ch:
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			return
		}
	case <-timer.C: // 触发超时机制，进行超时处理
		reply.Err = ErrWrongLeader
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.

	// 1.获取当前server的状态，判断是不是分片控制器集群中的leader，
	// 如果不是leader，说明client发错了节点，返回一个错误码给client，client重发
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 2.将Op封装并传到下一层，添加到日志中，并利用raft分布式共识算法进行同步
	op := Op{OpType: LeaveType, SeqId: args.SeqId, ClientId: args.ClientId, LeaveGids: args.GIDs}
	lastIndex, _, _ := sc.rf.Start(op)

	// 3.获取由下层raft通过chan传过来的信息，这里主要是为lastIndex对应的位置分配存储空间，并利用下面select模型监听ch通道
	ch := sc.getWaitCh(lastIndex)
	defer func() { // 处理完了之后需要将make出来的空间根据key来释放掉
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex)
		sc.mu.Unlock()
	}()

	// 4.设置超时时间（创建了一个定时器，每间隔设定的时间便会向channel发送一个时间事件）
	timer := time.NewTicker(LeaveOverTime * time.Millisecond)
	defer timer.Stop() // 这里推迟stop直到下面的<-timer.C发生为止（退出当前函数）

	// 5.利用select进行通信，获取chan中传过来的replyOp，进行判断是否Get成功
	select {
	case replyOp := <-ch:
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			return
		}
	case <-timer.C: // 触发超时机制，进行超时处理
		reply.Err = ErrWrongLeader
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.

	// 1.获取当前server的状态，判断是不是分片控制器集群中的leader，
	// 如果不是leader，说明client发错了节点，返回一个错误码给client，client重发
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 2.将Op封装并传到下一层，添加到日志中，并利用raft分布式共识算法进行同步
	op := Op{OpType: MoveType, SeqId: args.SeqId, ClientId: args.ClientId, MoveShard: args.Shard, MoveGid: args.GID}
	lastIndex, _, _ := sc.rf.Start(op)

	// 3.获取由下层raft通过chan传过来的信息，这里主要是为lastIndex对应的位置分配存储空间，并利用下面select模型监听ch通道
	ch := sc.getWaitCh(lastIndex)
	defer func() { // 处理完了之后需要将make出来的空间根据key来释放掉
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex)
		sc.mu.Unlock()
	}()

	// 4.设置超时时间（创建了一个定时器，每间隔设定的时间便会向channel发送一个时间事件）
	timer := time.NewTicker(MoveOverTime * time.Millisecond)
	defer timer.Stop() // 这里推迟stop直到下面的<-timer.C发生为止（退出当前函数）

	// 5.利用select进行通信，获取chan中传过来的replyOp，进行判断是否Get成功
	select {
	case replyOp := <-ch:
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			reply.Err = OK
			return
		}
	case <-timer.C: // 触发超时机制，进行超时处理
		reply.Err = ErrWrongLeader
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.

	// 1.获取当前server的状态，判断是不是分片控制器集群中的leader，
	// 如果不是leader，说明client发错了节点，返回一个错误码给client，client重发
	_, ifLeader := sc.rf.GetState()
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// 2.将Op封装并传到下一层，添加到日志中，并利用raft分布式共识算法进行同步
	op := Op{OpType: QueryType, SeqId: args.SeqId, ClientId: args.ClientId, QueryNum: args.Num}
	lastIndex, _, _ := sc.rf.Start(op)

	// 3.获取由下层raft通过chan传过来的信息，这里主要是为lastIndex对应的位置分配存储空间，并利用下面select模型监听ch通道
	ch := sc.getWaitCh(lastIndex)
	defer func() { // 处理完了之后需要将make出来的空间根据key来释放掉
		sc.mu.Lock()
		delete(sc.waitChMap, lastIndex)
		sc.mu.Unlock()
	}()

	// 4.设置超时时间（创建了一个定时器，每间隔设定的时间便会向channel发送一个时间事件）
	timer := time.NewTicker(QueryOverTime * time.Millisecond)
	defer timer.Stop() // 这里推迟stop直到下面的<-timer.C发生为止（退出当前函数）

	// 5.利用select进行通信，获取chan中传过来的replyOp，进行判断是否Get成功
	select {
	case replyOp := <-ch:
		if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
			reply.Err = ErrWrongLeader
		} else {
			sc.mu.Lock()
			reply.Err = OK
			sc.seqMap[op.ClientId] = op.SeqId                        // 更新客户端的请求序列号
			if op.QueryNum == -1 || op.QueryNum >= len(sc.configs) { // 如果请求的配置信息版本号不符合要求，直接将最新的版本信息返回回去
				reply.Config = sc.configs[len(sc.configs)-1]
			} else {
				reply.Config = sc.configs[op.QueryNum]
			}
			sc.mu.Unlock()
			return
		}
	case <-timer.C: // 触发超时机制，进行超时处理
		reply.Err = ErrWrongLeader
	}
}

// ------------------------------------------------------------------handler部分-----------------------------------------------------

// The shardctrler should react by creating a new configuration that includes the new replica groups. The new
//configuration should divide the shards as evenly as possible among the full set of groups, and should move as few
//shards as possible to achieve that goal. The shardctrler should allow re-use of a GID if it's not part of the
//current configuration (i.e. a GID should be allowed to Join, then Leave, then Join again).
// 处理join进来的gids，需要负载均衡处理（可能存在同时增加多个组）
func (sc *ShardCtrler) JoinHandler(servers map[int][]string) *Config {
	// 1.从sc.configs中取出最新的版本配置信息
	num := len(sc.configs)
	lastConfig := sc.configs[num-1]

	// 2.将需要join进来的gid添加到最新的版本配置信息中，创建一个新的配置信息
	newGroups := make(map[int][]string)
	for gid, serverCluster := range lastConfig.Groups {
		newGroups[gid] = serverCluster
	}
	for gid, serverCluster := range servers {
		newGroups[gid] = serverCluster
	}

	// 3.创建一个GroupMap并初始化，用于记录每个分组有几个分片
	groupMap := make(map[int]int)
	for gid := range newGroups {
		groupMap[gid] = 0
	}

	// 4.先统计lastConfig中每个分组有几个分片
	for _, gid := range lastConfig.Shards {
		if gid != 0 {
			groupMap[gid]++
		}
	}

	// 5.将需要join的组添加到最新的版本配置信息中
	if len(groupMap) == 0 { // 如果原本就没有shard,不需要负载均衡处理
		return &Config{
			Num:    num,
			Shards: [NShards]int{},
			Groups: newGroups,
		}
	} else { // 需要负载均衡处理
		return &Config{
			Num:    num,
			Shards: sc.loadBalance(groupMap, lastConfig.Shards),
			Groups: newGroups,
		}
	}
}

// 处理需要leave的gids，需要负载均衡处理
func (sc *ShardCtrler) LeaveHandler(gids []int) *Config {
	// 1.从sc.configs中取出最新的版本配置信息
	num := len(sc.configs)
	lastConfig := sc.configs[num-1]

	// 2.创建一个新的newGroups，用于存放修改后的servers
	newGroups := make(map[int][]string)
	for gid, serverCluster := range lastConfig.Groups {
		newGroups[gid] = serverCluster
	}

	// 3.将需要删除的gids从newGroups中删除
	for _, gid := range gids {
		delete(newGroups, gid)
	}

	// 4.先做标记，标记有哪些组已经被删除了
	leaveMap := make(map[int]bool)
	for _, gid := range gids {
		leaveMap[gid] = true
	}

	// 5.创建初始化一个groupMap，用于记录gid存放了多少个shard;创建一个newShard，用于更新因leave导致shard需要发生变动
	groupMap := make(map[int]int)
	for gid := range newGroups {
		groupMap[gid] = 0
	}
	newShards := lastConfig.Shards

	// 6.统计每个group有多少个shard
	for shard, gid := range lastConfig.Shards {
		if leaveMap[gid] { // group被删除了，需要暂时将shard对应的gid设置成InvalidGid作为标记，便于后续的负载均衡处理
			newShards[shard] = InvalidGid
		} else { // 没有被删除需要group需要统计shard的个数
			groupMap[gid]++
		}
	}

	// 7.返回move之后的配置信息版本
	if len(groupMap) == 0 { // 如果所有的group都被删除了
		return &Config{
			Num:    num,
			Shards: [NShards]int{},
			Groups: newGroups,
		}
	} else { // 需要做负载均衡处理
		return &Config{
			Num:    num,
			Shards: sc.loadBalance(groupMap, newShards),
			Groups: newGroups,
		}
	}
}

// 处理需要move的shard，这里不需要做负载均衡处理
func (sc *ShardCtrler) MoveHandler(gid int, shard int) *Config {
	// 1.获取最新版本的配置信息
	num := len(sc.configs)
	lastConfig := sc.configs[num-1]

	// 2.创建newGroup和newShard
	newGroups := make(map[int][]string)
	for gid, serverCluster := range lastConfig.Groups {
		newGroups[gid] = serverCluster
	}
	newShards := lastConfig.Shards

	// 3.更改newShard中shard指定的gid
	newShards[shard] = gid

	// 4.返回move之后的最新版本的配置信息
	return &Config{
		Num:    num,
		Shards: newShards,
		Groups: newGroups,
	}
}

// ------------------------------------------------------------------loop部分-----------------------------------------------------

// 处理applyCh发送过来的ApplyMsg
func (sc *ShardCtrler) applyMsgHandlerLoop() {
	// 利用select循环监听channel中的信息，并根据ApplyMsg的类型进行不同的处理
	for {
		select {
		case msg := <-sc.applyCh:
			// 1.如果ApplyMsg合理
			if msg.CommandValid {
				// 1.1.获取ApplyMsg在waitCh中的index，后续要根据index将处理之后的Op添加到waitCh中
				index := msg.CommandIndex
				op := msg.Command.(Op) // 判断是否是指定类型的数据，如果是就转化成指定类型的数据，不是就panic(这里的)

				// 1.2.判断是否是重复的请求序列号
				if !sc.ifDuplicate(op.ClientId, op.SeqId) {
					// 1.2.1.如果不是重复的请求序列号，调用不同的处理函数
					sc.mu.Lock()

					switch op.OpType {
					case JoinType:
						sc.configs = append(sc.configs, *sc.JoinHandler(op.JoinServers))
					case LeaveType:
						sc.configs = append(sc.configs, *sc.LeaveHandler(op.LeaveGids))
					case MoveType:
						sc.configs = append(sc.configs, *sc.MoveHandler(op.MoveGid, op.MoveShard))
					}

					// 1.2.2.需要更新seqMap
					sc.seqMap[op.ClientId] = op.SeqId

					sc.mu.Unlock()
				}

				// 1.3.将对应的ApplyMsg添加到waitCh，然后根据ApplyMsg的类型处理之后返回给client
				sc.getWaitCh(index) <- op
			}
		}
	}
}

// ------------------------------------------------------------------utils部分-----------------------------------------------------

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) ifDuplicate(clientId int64, seqId int) bool {
	// 1.加锁并推迟锁的释放
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// 2.获取对应的clientId的信息
	lastSeqId, exist := sc.seqMap[clientId]

	// 3.判断是否重复
	if !exist {
		return false
	}
	return seqId <= lastSeqId
}

func (sc *ShardCtrler) getWaitCh(index int) chan Op {
	// 1.加锁并推迟锁的释放
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// 2.根据index获取对应等待应用的命令
	ch, exist := sc.waitChMap[index]
	if !exist {
		sc.waitChMap[index] = make(chan Op, 1)
		ch = sc.waitChMap[index]
	}

	// 3.返回获取的ch
	return ch
}

// 解决负载均衡的基本思路：
// 1、计算出平均每个group应该对应几个shard，注意这里的平均是一个整数
// 2、因为不可能完全平均，有一部分会存在平均数+1的情况，计算出有几个这样的group
// 3、对group根据shard的个数进行排序，用数组记录group的次序
// 4、将超过平均值的（这个平均值可能是平均值+1）group单独找出来，然后将超出部分的shard对应的gid标记为InvalidGid
// 5、将标记为InvalidGid的shard重新分配给那些少于平均值的group
func (sc *ShardCtrler) loadBalance(GroupMap map[int]int, lastShards [NShards]int) [NShards]int {
	// 1.计算平均值以及（平均值+1）的group个数
	groupNum := len(GroupMap)
	average := NShards / groupNum
	remainder := NShards % groupNum

	// 2.对group按照shard的个数进行快速排序，对排序后的结果用数组表示
	sortGids := sortGroupShard(GroupMap)

	// 3.找出所有超出平均值部分的Group，将多的shard释放出来
	for index, gid := range sortGids {
		// 3.1.计算第index个group的实际shard应该为多少
		averageGid := average
		if moreAllocations(remainder, index) {
			averageGid += 1
		}

		// 3.2.判断gid的目前的shard是否大于averageGid
		if GroupMap[gid] > averageGid {
			for indexShard, shardGid := range lastShards {
				if shardGid == gid {
					lastShards[indexShard] = InvalidGid
					GroupMap[gid]--
				}

				if GroupMap[gid] == averageGid {
					break
				}
			}
		}
	}

	// 4.找出所有低于平均值部分的Group，分配对应的shard
	for index, gid := range sortGids {
		// 4.1.计算第index个group的实际shard应该为多少
		averageGid := average
		if moreAllocations(remainder, index) {
			averageGid += 1
		}

		// 4.2.判断gid的目前的shard是否小于averageGid
		if GroupMap[gid] < averageGid {
			for indexShard, shardGid := range lastShards {
				if shardGid == InvalidGid {
					lastShards[indexShard] = gid
					GroupMap[gid]++
				}

				if GroupMap[gid] == averageGid {
					break
				}
			}
		}
	}

	return lastShards
}

// 对group进行排序
func sortGroupShard(GroupMap map[int]int) []int {
	// 1.用数组存放gids
	sortGids := make([]int, 0)
	for gid := range GroupMap {
		sortGids = append(sortGids, gid)
	}

	// 2.利用快速排序对sortGids排序
	quickSort(sortGids, 0, len(sortGids)-1, GroupMap)

	// 3.返回排序好的sortGids
	return sortGids
}

// 快速排序，升序（要求如果两个group的shard个数相同时，group编号小的需要排在前面）
func quickSort(sortGids []int, left, right int, GroupMap map[int]int) {
	if left >= right {
		return
	}
	pivot := sortGids[left]
	i, j := left, right
	for i < j {
		for i < j && (GroupMap[sortGids[j]] < GroupMap[pivot] || (GroupMap[sortGids[j]] == GroupMap[pivot] && sortGids[j] > pivot)) {
			j--
		}
		for i < j && (GroupMap[sortGids[i]] > GroupMap[pivot] || (GroupMap[sortGids[i]] == GroupMap[pivot] && sortGids[i] <= pivot)) {
			i++
		}
		sortGids[i], sortGids[j] = sortGids[j], sortGids[i]
	}
	sortGids[left], sortGids[i] = sortGids[i], sortGids[left]
	quickSort(sortGids, left, i-1, GroupMap)
	quickSort(sortGids, i+1, right, GroupMap)
}

// 判断排序好的sortGids中第i个group对应的平均值是否为整数平均值+1
func moreAllocations(remainder int, index int) bool {
	if index < remainder {
		return true
	} else {
		return false
	}
}

// ------------------------------------------------------------------输出日志部分-----------------------------------------------------

func (sc *ShardCtrler) printJoinHandler() error {
	//result := "Server_" + fmt.Sprintf("%d", sc.me) + ".txt"
	result := "Server.txt"
	// 打开文件，如果文件不存在就创建
	file, err := os.OpenFile(result, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := fmt.Fprintf(file, "\n"); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(file, "\n"); err != nil {
		return err
	}

	return nil
}

func (sc *ShardCtrler) printLeaveHandler() error {
	//result := "Server_" + fmt.Sprintf("%d", sc.me) + ".txt"
	result := "Server.txt"
	// 打开文件，如果文件不存在就创建
	file, err := os.OpenFile(result, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := fmt.Fprintf(file, "\n"); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(file, "\n"); err != nil {
		return err
	}

	return nil
}

func (sc *ShardCtrler) printMoveHandler() error {
	//result := "Server_" + fmt.Sprintf("%d", sc.me) + ".txt"
	result := "Server.txt"
	// 打开文件，如果文件不存在就创建
	file, err := os.OpenFile(result, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := fmt.Fprintf(file, "\n"); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(file, "\n"); err != nil {
		return err
	}

	return nil
}

func (sc *ShardCtrler) printQueryHandler() error {
	//result := "Server_" + fmt.Sprintf("%d", sc.me) + ".txt"
	result := "Server.txt"
	// 打开文件，如果文件不存在就创建
	file, err := os.OpenFile(result, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := fmt.Fprintf(file, "\n"); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(file, "\n"); err != nil {
		return err
	}

	return nil
}
