package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// -------------------------------------------------结构体定义部分-----------------------------------------

// Op要能接收上层client发送过来的参数，也要能够转接raft服务层传回来的command
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	// client
	SeqId    int
	ClientId int64
	Key      string
	Value    string
	// raft
	Index int // raft服务层传来的index

	OpType string // put/append/get
}

type KVServer struct {
	mu      sync.Mutex         // 加锁
	me      int                // 服务器id
	rf      *raft.Raft         // 服务器对应的raft共识
	applyCh chan raft.ApplyMsg // 应用消息的通道
	dead    int32              // set by Kill() // 服务器是否挂掉

	maxraftstate int // snapshot if log grows this big(就是说如果raftstate的大小达到这个最大值时，需要生成快照)

	// Your definitions here.
	seqMap    map[int64]int     // 记录clientId:seqId，确保服务器对于客户端请求只执行一次（需要持久化）
	waitChMap map[int]chan Op   // 传递由下层Raft服务的applyCh传递过来的command,index:chan(op)，index为command在raft日志中的index
	kvPersist map[string]string // 存储持久化的KV键值对，key:value（需要持久化）

	lastIncludeIndex int // raft对应的点，应用了raft的日志快照
}

// -------------------------------------------------初始化（Start）部分-----------------------------------------
//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	// 这里的applyCh用来server和与其对应的raft进行消息传递（主要是raft将ApplyMsg传递给上层的）
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.seqMap = make(map[int64]int)
	kv.kvPersist = make(map[string]string)
	kv.waitChMap = make(map[int]chan Op)

	kv.lastIncludeIndex = 0
	// 因为可能会crash重连
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		kv.DecodeSnapShot(snapshot)
	}
	// 启动协程
	go kv.applyMsgHandlerLoop()

	return kv
}

// -------------------------------------------------RPC部分-----------------------------------------

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		ClientId: args.CliendId,
		SeqId:    args.SeqId,
		Key:      args.Key,
		OpType:   "Get",
	}

	// 1.通过args中的key来获取index
	lastIndex, _, _ := kv.rf.Start(op)
	//fmt.Printf("[++++Server[%v]++++]:reply a Get,lastIndex:%v,op is :%+v\n", kv.me, lastIndex, op)
	//kv.printServerRPCStart(lastIndex, "Get", op)

	// 2.通过index从waitChMap中获取
	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, lastIndex)
		kv.mu.Unlock()
	}()

	// 3.设置超时计时器
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()

	// 4.利用select进行通信，获取chan中传过来的replyOp，进行判断是否Get成功
	select {
	case replyOp := <-ch:
		{
			//kv.printServerGetEnd(*args, op, replyOp)
			//fmt.Printf("[++++Server[%v]++++]:receive a GetAsk:%+v,op is :%+v,replyOp:%+v\n", kv.me, args, op, replyOp)
			if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
				reply.Err = ErrWrongLeader
			} else {
				reply.Err = OK

				kv.mu.Lock()
				reply.Value = kv.kvPersist[args.Key]
				kv.mu.Unlock()
				return
			}
		}
	case <-timer.C:
		{
			reply.Err = ErrWrongLeader
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() { // 如果server被killed，则返回访问错误的leader
		reply.Err = ErrWrongLeader
		return
	}

	_, isLeader := kv.rf.GetState() // 返回raft的任期和状态（判断当前的server是否为leader）
	if !isLeader {                  // 如果不是leader
		reply.Err = ErrWrongLeader
		return
	}

	// 封装Op传到下一层start
	op := Op{
		ClientId: args.ClientId,
		SeqId:    args.SeqId,
		Key:      args.Key,
		Value:    args.Value,
		OpType:   args.Op,
	}
	// 1.获取index
	lastIndex, _, _ := kv.rf.Start(op) // 返回添加command对应的index
	//fmt.Printf("[++++Server[%v]++++]:reply a PutAppend,lastIndex:%v,op is :%+v\n", kv.me, lastIndex, op)
	//kv.printServerRPCStart(lastIndex, "PutAppend", op)

	// 2.通过lastIndex获取waitCh
	ch := kv.getWaitCh(lastIndex)
	defer func() {
		kv.mu.Lock()
		delete(kv.waitChMap, lastIndex) // 将lastIndex对应的chan op删除掉
		kv.mu.Unlock()
	}()

	// 3.设置超时ticker
	timer := time.NewTicker(100 * time.Millisecond)
	defer timer.Stop()

	// 4.利用select进行通信，获取chan中传过来的replyOp
	select {
	case replyOp := <-ch: // 通过chan获取处理后的回复
		{
			//kv.printServerPutAppendEnd(*args, op, replyOp)
			//fmt.Printf("[++++Server[%v]++++]:receive a PutAppendAsk:%+v,op is :%+v,replyOp:%+v\n", kv.me, args, op, replyOp)
			// 通过clientId和seqId确定唯一的操作序列
			if op.ClientId != replyOp.ClientId || op.SeqId != replyOp.SeqId {
				reply.Err = ErrWrongLeader
			} else {
				reply.Err = OK
				return
			}
		}
	case <-timer.C:
		{
			reply.Err = ErrWrongLeader
		}
	}
}

// -------------------------------------------------Loop部分-----------------------------------------
// 处理applyCh发送过来的ApplyMsg(对应用消息的处理)
func (kv *KVServer) applyMsgHandlerLoop() {
	for {
		if kv.killed() { // 如果kvdead，则直接退出
			return
		}

		select { // 利用select模型进行通信
		// 1、client根据请求调用server的Get/PutAppend接口，然后server根据对应接口将请求传递给raft，
		// 2、raft再将已经处理过并应用到状态机中的日志对应的消息通过chan传递回来，
		// 3、server利用select模型接收chan中的消息，并对消息做处理，放置到waitCh中，
		// 4、server的Get/PutAppend接口也同样使用select模型进行监听waitCh通道，根据相应的请求返回对应的结果给client
		case msg := <-kv.applyCh:
			{ // 如果应用通道的有消息了
				if msg.CommandValid { // 如果commandValid为合理的命令，说明这个通道的消息是command，否则是snapshot
					if msg.CommandIndex <= kv.lastIncludeIndex {
						return
					}

					index := msg.CommandIndex
					// 先进行assert，判断是否是指定类型的数据，如果是就转化成指定类型的数据，不是就panic(这里的)
					op := msg.Command.(Op)
					// 判断是否出现了重复项，只处理非重复的
					if !kv.ifDuplicate(op.ClientId, op.SeqId) {
						kv.mu.Lock()

						switch op.OpType { // 对于“Get”操作只需要将op的结果放置到waitCh中即可
						case "Put":
							{
								kv.kvPersist[op.Key] = op.Value
							}
						case "Append":
							{
								kv.kvPersist[op.Key] += op.Value
							}
						}

						kv.seqMap[op.ClientId] = op.SeqId // 更新seqId
						kv.mu.Unlock()
					}

					// 如果需要snapshot，且超出其stateSize
					if kv.maxraftstate != -1 && kv.rf.GetRaftStateSize() > kv.maxraftstate {
						snapshot := kv.PersistSnapShot()
						kv.rf.Snapshot(msg.CommandIndex, snapshot)
					}

					// 将返回的ch返回waitch
					kv.getWaitCh(index) <- op
				}

				if msg.SnapshotValid { // 如果snapshotValid说明应用通道中的消息是快照消息
					kv.mu.Lock()
					// 判断此时有没有竞争
					if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
						// 读取快照的数据
						kv.DecodeSnapShot(msg.Snapshot)
						kv.lastIncludeIndex = msg.SnapshotIndex
					}

					kv.mu.Unlock()
				}
			}
		}
	}
}

// -------------------------------------------------持久化部分-----------------------------------------
// 对存储的kv键值对和client向server发送的请求序列进行反序列化
func (kv *KVServer) DecodeSnapShot(snapshot []byte) {
	if snapshot == nil || len(snapshot) == 0 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var kvPersist map[string]string
	var seqMap map[int64]int

	if d.Decode(&kvPersist) != nil || d.Decode(&seqMap) != nil {
		fmt.Printf("[Server(%v)] Failed to decode snapshot!!", kv.me)
	} else {
		kv.kvPersist = kvPersist
		kv.seqMap = seqMap
	}
}

// 对存储的kv键值对和client向server发送的请求序列需要序列化
func (kv *KVServer) PersistSnapShot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	e.Encode(kv.kvPersist)
	e.Encode(kv.seqMap)
	data := w.Bytes()

	return data
}

// -------------------------------------------------utils部分-----------------------------------------
//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) ifDuplicate(clientId int64, seqId int) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	lastSeqId, exist := kv.seqMap[clientId]
	if exist && seqId <= lastSeqId {
		return true
	}

	return false
}

func (kv *KVServer) getWaitCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	waitCh, exist := kv.waitChMap[index]
	if !exist { // 如果没有则，创建一个
		kv.waitChMap[index] = make(chan Op, 1)
		waitCh = kv.waitChMap[index]
	}

	return waitCh
}

// 将server的RPC信息输出(start)
func (kv *KVServer) printServerRPCStart(lastIndex int, opType string, op Op) error {
	result := "Server_" + fmt.Sprintf("%d", kv.me) + ".txt"
	// 打开文件，如果文件不存在就创建
	file, err := os.OpenFile(result, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := fmt.Fprintf(file, "[++++Server[%v]++++]:reply a %v,lastIndex:%v,op is :%+v\n", kv.me, opType, lastIndex, op); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(file, "\n"); err != nil {
		return err
	}

	return nil
}

// 将server的Get信息输出(end)
func (kv *KVServer) printServerGetEnd(args GetArgs, op Op, replyOp Op) error {
	result := "Server_" + fmt.Sprintf("%d", kv.me) + ".txt"
	// 打开文件，如果文件不存在就创建
	file, err := os.OpenFile(result, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := fmt.Fprintf(file, "[++++Server[%v]++++]:receive a GetReply:%+v,op is :%+v,replyOp:%+v\n", kv.me, args, op, replyOp); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(file, "\n"); err != nil {
		return err
	}

	return nil
}

// 将server的PutAppend信息输出(end)
func (kv *KVServer) printServerPutAppendEnd(args PutAppendArgs, op Op, replyOp Op) error {
	result := "Server_" + fmt.Sprintf("%d", kv.me) + ".txt"
	// 打开文件，如果文件不存在就创建
	file, err := os.OpenFile(result, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := fmt.Fprintf(file, "[++++Server[%v]++++]:receive a PutAppendReply:%+v,op is :%+v,replyOp:%+v\n", kv.me, args, op, replyOp); err != nil {
		return err
	}

	if _, err := fmt.Fprintf(file, "\n"); err != nil {
		return err
	}

	return nil
}
