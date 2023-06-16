package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new logs entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the logs, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"

	"time"
)

//
// as each Raft peer becomes aware that successive logs entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyChan passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed logs entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyChan, but set CommandValid to false for these
// other uses.
//

// Status 节点的角色
type Status int

// VoteState 投票的状态 2A
type VoteState int

// AppendEntriesState 追加日志的状态 2A 2B
type AppendEntriesState int

// InstallSnapshotState 安装快照的状态
type InstallSnapshotState int

// 枚举节点的类型：跟随者、竞选者、领导者
const (
	Follower Status = iota
	Candidate
	Leader
)

var FileName string = "output"

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (

	// MoreVoteTime MinVoteTime 定义随机生成投票过期时间范围:(MoreVoteTime+MinVoteTime~MinVoteTime)
	MoreVoteTime = 100 //100 20
	MinVoteTime  = 75  // 75 20

	// HeartbeatSleep 心脏休眠时间,要注意的是，这个时间要比选举低，才能建立稳定心跳机制
	HeartbeatSleep = 35 // 35 10
	AppliedSleep   = 15 // 15 5
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's status
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted status
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// 所有的servers需要持久化的变量:
	currentTerm int        // 记录当前的任期
	votedFor    int        // 记录当前的任期把票投给了谁
	logs        []LogEntry // 日志条目数组，包含了状态机要执行的指令集，以及收到领导时的任期号

	// 对于正常流程来说应该先applyLog进chan里，然后更新commit，最后两者应该是相同的，只是先后更新顺序不同
	commitIndex int // 最新被提交日志的index，下标从1开始
	lastApplied int // 最新被应用到状态机中日志的index，下标从1开始

	// nextIndex与matchIndex初始化长度应该为len(peers)，Leader对于每个Follower都记录他的nextIndex和matchIndex
	// nextIndex指的是下一个的appendEntries要从哪里开始
	// matchIndex指的是已知的某follower的log与leader的log最大匹配到第几个Index,已经apply
	nextIndex  []int // 对于每一个server，需要发送给他下一个日志条目的索引值（初始化为leader日志index+1,那么范围就对标len），下标从1开始
	matchIndex []int // 对于每一个server，已经复制给该server的最后日志条目下标，下标从1开始

	applyChan chan ApplyMsg // 用来写入通道（将ApplyMsg类型的信息写入到通道中）

	// paper外自己追加的
	status              Status    // 节点的状态（leader/candidate/follower）
	voteNum             int       // 记录在成为候选人时收到的票数
	votedTimer          time.Time // 记录最新接收到心跳/日志添加/投票的时间
	firstNoOp           int       // 在成为了leader之后是否发送了空操作给所有其他的节点（成为leader之后发送空操作，避免因为网络分区导致已经提交的日志被覆盖掉）
	firstNoOpSuccessNum int       // 提交空操作成功的计数，判断是否将空操作提交给了大多数节点

	// 2D中用于传入快照点
	lastIncludeIndex int // 包含在快照中的最后一条日志的index，下标从1开始
	lastIncludeTerm  int // 包含在快照中的最后一条日志的term
}

type LogEntry struct {
	Index   int         // 日志的索引
	Term    int         // 日志的任期
	Command interface{} // 日志保存的命令，由上层应用定义
}

// --------------------------------------------------------RPC参数部分----------------------------------------------------

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //	需要竞选的人的任期
	CandidateId  int // 需要竞选的人的Id
	LastLogIndex int // 竞选人日志条目最后索引(2D包含快照
	LastLogTerm  int // 候选人最后日志条目的任期号(2D包含快照
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 投票方的term，如果竞选者比自己还低就改为这个
	VoteGranted bool // 是否投票给了该竞选人
}

// AppendEntriesArgs Append Entries RPC structure
type AppendEntriesArgs struct {
	Term         int        // leader的任期
	LeaderId     int        // leader自身的ID
	PrevLogIndex int        // 用于匹配日志位置是否是合适的，初始化rf.nextIndex[i] - 1
	PrevLogTerm  int        // 用于匹配日志的任期是否是合适的是，是否有冲突
	Entries      []LogEntry // 预计存储的日志（为空时就是心跳连接）
	LeaderCommit int        // leader的commit index指的是最后一个被大多数机器都复制的日志Index
}

type AppendEntriesReply struct {
	Term        int  // leader的term可能是过时的，此时收到的Term用于更新他自己
	Success     bool //	如果follower与Args中的PreLogIndex/PreLogTerm都匹配才会接过去新的日志（追加），不匹配直接返回false
	UpNextIndex int  // 如果发生conflict时reply传过来的正确的下标用于更新nextIndex[i]
}

type InstallSnapshotArgs struct {
	Term             int    // 发送请求方的任期(leader的任期)
	LeaderId         int    // 请求方的LeaderId(follower的leader)
	LastIncludeIndex int    // 快照最后applied的日志下标
	LastIncludeTerm  int    // 快照最后applied时的当前任期
	Data             []byte // 快照区块的原始字节流数据
	//Done bool
}

type InstallSnapshotReply struct {
	Term int
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent status, and also initially holds the most
// recent saved status, if any. applyChan is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.mu.Lock()

	rf.status = Follower
	rf.currentTerm = 0
	rf.voteNum = 0
	rf.votedFor = -1
	rf.firstNoOp = 0
	rf.firstNoOpSuccessNum = 0

	rf.lastApplied = 0
	rf.commitIndex = 0

	rf.lastIncludeIndex = 0
	rf.lastIncludeTerm = 0

	rf.logs = []LogEntry{}
	//rf.logs = append(rf.logs, LogEntry{})
	rf.applyChan = applyCh

	rf.printRaft("Make Start")

	rf.mu.Unlock()

	// initialize from status persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// 同步快照信息
	if rf.lastIncludeIndex > 0 {
		rf.lastApplied = rf.lastIncludeIndex
	}

	go rf.electionTicker()

	go rf.appendTicker()

	go rf.committedTicker()

	return rf
}

// --------------------------------------------------------undefined----------------------------------------------------
// 生成一个随机的超时时间
func generateOverTime(nodeID int64) int64 {
	// 生成一个随机数，用于计算超时时间
	rand.Seed(time.Now().UnixNano() + nodeID)
	randomNum := rand.Intn(MoreVoteTime) + MinVoteTime // 在300~600ms范围内生成一个随机数

	// 返回随机时间
	return int64(randomNum)
}

func (rf *Raft) UpToDate(lastLogIndex int, lastLogTerm int) bool {
	lastLog := rf.getLastLog()
	lastIndex := rf.getLastIndex()
	if lastLog.Term > lastLogTerm {
		return false
	} else if lastLog.Term == lastLogTerm && lastIndex > lastLogIndex {
		return false
	}
	return true
}

// 获取最后一条日志
func (rf *Raft) getLastLog() LogEntry {
	lastIndex := rf.getLastIndex()
	if (lastIndex - rf.lastIncludeIndex) == 0 {
		return LogEntry{}
	}
	return rf.logs[lastIndex-rf.lastIncludeIndex-1]
}

// 获取最后一条日志的索引
func (rf *Raft) getLastIndex() int {
	return len(rf.logs) + rf.lastIncludeIndex
}

// 获取最后一条日志的任期
func (rf *Raft) getLastTerm() int {
	LogEntry := rf.getLastLog()
	return LogEntry.Term
}

func (rf *Raft) restoreLog(index int) LogEntry {
	if index <= 0 || index > rf.getLastIndex() {
		return LogEntry{}
	} else if index <= rf.lastIncludeIndex {
		return LogEntry{}
	} else {
		return rf.logs[index-rf.lastIncludeIndex-1]
	}
}

// 获取指定日志的term
func (rf *Raft) restoreLogTerm(index int) int {
	if index == rf.lastIncludeIndex {
		return rf.lastIncludeTerm
	} else {
		logEntry := rf.restoreLog(index)
		return logEntry.Term
	}
}

func (rf *Raft) getPrevLogInfo(server int) (int, int) {
	// 获取 follower 的下一条日志索引
	nextIndex := rf.nextIndex[server]

	// 如果 follower 的 nextIndex <= rf.lastIncludeIndex，说明该 follower 的日志都已经被快照包含了
	if nextIndex <= rf.lastIncludeIndex {
		return rf.lastIncludeIndex, rf.lastIncludeTerm
	}

	// 否则，获取 prevLogIndex 和 prevLogTerm
	prevLogIndex := nextIndex - 1
	prevLogTerm := rf.restoreLogTerm(prevLogIndex)

	return prevLogIndex, prevLogTerm
}

func min(a int, b int) int {
	if a > b {
		return b
	} else {
		return a
	}
}

func (rf *Raft) printRaft(event string) error {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	//log.Println("printRaft")
	// 打开文件，如果文件不存在就创建
	/*
		result := FileName + fmt.Sprintf("%d", rf.me) + ".txt"
		file, err := os.OpenFile(result, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
		defer file.Close()

		if _, err := fmt.Fprintf(file, "<-------------------------%s------------------->\n", event); err != nil {
			return err
		}

		// 将格式化字符串输出到文件
		if _, err := fmt.Fprintf(file, "-----------------------------Raft-----------------------\n"); err != nil {
			return err
		}

		if _, err := fmt.Fprintf(file, "rf.me:%d\n", rf.me); err != nil {
			return err
		}

		if _, err := fmt.Fprintf(file, "rf.status:%d\n", rf.status); err != nil {
			return err
		}

		if _, err := fmt.Fprintf(file, "rf.votedFor:%d\n", rf.votedFor); err != nil {
			return err
		}

		if _, err := fmt.Fprintf(file, "rf.currentTerm:%d\n", rf.currentTerm); err != nil {
			return err
		}

		if _, err := fmt.Fprintf(file, "rf.commitIndex:%d\n", rf.commitIndex); err != nil {
			return err
		}

		if _, err := fmt.Fprintf(file, "rf.lastApplied:%d\n", rf.lastApplied); err != nil {
			return err
		}

		if _, err := fmt.Fprintf(file, "rf.lastIncludeIndex:%d\n", rf.lastIncludeIndex); err != nil {
			return err
		}

		if _, err := fmt.Fprintf(file, "rf.lastIncludeTerm:%d\n", rf.lastIncludeTerm); err != nil {
			return err
		}

		if rf.status == Leader {
			if _, err := fmt.Fprintf(file, "rf.firstNoOp:%d,rf.firstNoOpNum:%d\n", rf.firstNoOp, rf.firstNoOpSuccessNum); err != nil {
				return err
			}

			if _, err := fmt.Fprintf(file, "nextIndex:"); err != nil {
				return err
			}
			for _, nextIndex := range rf.nextIndex {
				if _, err := fmt.Fprintf(file, "%d ", nextIndex); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprintf(file, "\n"); err != nil {
				return err
			}

			if _, err := fmt.Fprintf(file, "matchIndex:"); err != nil {
				return err
			}
			for _, matchIndex := range rf.matchIndex {
				if _, err := fmt.Fprintf(file, "%d ", matchIndex); err != nil {
					return err
				}
			}
			if _, err := fmt.Fprintf(file, "\n"); err != nil {
				return err
			}
		}

		if _, err := fmt.Fprintf(file, "rf.logs:\n"); err != nil {
			return err
		}
		for _, logEntry := range rf.logs {
			if _, err := fmt.Fprintf(file, "{index:%d,term:%d,cmd:%d}", logEntry.Index, logEntry.Term, logEntry.Command); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintf(file, "\n"); err != nil {
			return err
		}

		if _, err := fmt.Fprintf(file, "<-------------------------%s------------------->\n", event); err != nil {
			return err
		}

		if _, err := fmt.Fprintf(file, "\n"); err != nil {
			return err
		}
	*/
	return nil
}

func (rf *Raft) printAppendEntriesArgs(args *AppendEntriesArgs) error {
	/*
		//rf.mu.Lock()
		result := FileName + fmt.Sprintf("%d", rf.me) + ".txt"
		//rf.mu.Unlock()

		// 打开文件，如果文件不存在就创建
		file, err := os.OpenFile(result, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			return err
		}
		defer file.Close()

		if len(args.Entries) == 0 {
			if _, err := fmt.Fprintf(file, "<-----------------------------心跳信息Start----------------------->\n"); err != nil {
				return err
			}
		} else {
			if _, err := fmt.Fprintf(file, "<-----------------------------日志添加Start----------------------->\n"); err != nil {
				return err
			}
		}

		if err := rf.printRaft("AppendEntries之前"); err != nil {
			return err
		}

		if _, err := fmt.Fprintf(file, "-----------------------------AppendEntriesArgs-----------------------\n"); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(file, "args.Term:%d\n", args.Term); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(file, "args.LeaderId:%d\n", args.LeaderId); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(file, "args.PrevLogIndex:%d\n", args.PrevLogIndex); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(file, "args.PrevLogTerm:%d\n", args.PrevLogTerm); err != nil {
			return err
		}
		if _, err := fmt.Fprintf(file, "args.LeaderCommit:%d\n", args.LeaderCommit); err != nil {
			return err
		}

		if _, err := fmt.Fprintf(file, "args.Entries:\n"); err != nil {
			return err
		}
		for _, logEntry := range args.Entries {
			if _, err := fmt.Fprintf(file, "{index:%d,term:%d,cmd:%d}", logEntry.Index, logEntry.Term, logEntry.Command); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintf(file, "\n"); err != nil {
			return err
		}

		if len(args.Entries) == 0 {
			if _, err := fmt.Fprintf(file, "<-----------------------------心跳信息End----------------------->\n"); err != nil {
				return err
			}
		} else {
			if _, err := fmt.Fprintf(file, "<-----------------------------日志添加End----------------------->\n"); err != nil {
				return err
			}
		}

		if _, err := fmt.Fprintf(file, "\n"); err != nil {
			return err
		}
	*/
	return nil
}

func (rf *Raft) printAppendEntriesReply(reply *AppendEntriesReply) error {
	//rf.mu.Lock()
	result := FileName + fmt.Sprintf("%d", rf.me) + ".txt"
	//rf.mu.Unlock()

	// 打开文件，如果文件不存在就创建
	file, err := os.OpenFile(result, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := fmt.Fprintf(file, "<-----------------------------AppendEntriesReplyStart----------------------->"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(file, "reply.Term:%d\n", reply.Term); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(file, "reply.Success:%t\n", reply.Success); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(file, "reply.UpNextIndex:%d\n", reply.UpNextIndex); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(file, "<-----------------------------AppendEntriesReplyEnd----------------------->"); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(file, "\n"); err != nil {
		return err
	}

	return nil
}

// --------------------------------------------------------ticker部分----------------------------------------------------
func (rf *Raft) electionTicker() {
	for rf.killed() == false {
		nowTime := time.Now()
		time.Sleep(time.Duration(generateOverTime(int64(rf.me))) * time.Millisecond)

		rf.mu.Lock()
		rf.printRaft("选举时间超时之前")
		// 时间过期发起选举
		// 此处的流程为每次每次votedTimer如果小于在sleep睡眠之前定义的时间，就代表没有votedTimer没被更新为最新的时间，则发起选举
		if rf.votedTimer.Before(nowTime) && rf.status != Leader {
			// 转变状态
			rf.status = Candidate
			rf.votedFor = rf.me
			rf.voteNum = 1
			// todo ? 在成为候选人之前先要发送prevote来判断自己是否在大多数分区中，避免无限制的增加任期
			// (这里修改成了只有真正成为了leader才会进行rf.currentTerm的自增，但发送选举消息的时候仍然是进行了加1的)
			//rf.currentTerm += 1
			rf.persist()

			rf.printRaft("准备发起选举之前")

			rf.sendElection()
			rf.votedTimer = time.Now()

		}
		rf.mu.Unlock()

	}
}

func (rf *Raft) appendTicker() {
	for rf.killed() == false {
		time.Sleep(HeartbeatSleep * time.Millisecond)
		rf.mu.Lock()
		if rf.status == Leader {

			//log.Printf("Peer %d(%d) :leader appendEntries\n", rf.me, rf.currentTerm)
			//for i := 0; i < len(rf.peers); i++ {
			//	log.Printf("rf.nextIndex[%d]:%d,", i, rf.nextIndex[i])
			//}
			//log.Printf("\n")

			rf.mu.Unlock()
			rf.leaderAppendEntries()
		} else {
			rf.mu.Unlock()
		}
	}
}

// committedTicker 是一个 goroutine，用于将已提交的条目应用于状态机。
func (rf *Raft) committedTicker() {
	for rf.killed() == false {
		// 睡眠一段时间以避免忙等待。
		time.Sleep(AppliedSleep * time.Millisecond)

		// 获取锁以访问共享状态。
		rf.mu.Lock()

		// 如果没有新的已提交条目，则释放锁并继续。
		if rf.lastApplied >= rf.commitIndex {
			rf.mu.Unlock()
			continue
		}

		// 应用所有尚未应用的已提交条目。
		Messages := make([]ApplyMsg, 0)
		for rf.lastApplied < rf.commitIndex && rf.lastApplied < rf.getLastIndex() {
			rf.lastApplied += 1
			Messages = append(Messages, ApplyMsg{
				CommandValid:  true,
				SnapshotValid: false, // 这不是快照，所以是false
				CommandIndex:  rf.lastApplied,
				Command:       rf.restoreLog(rf.lastApplied).Command,
			})
		}

		// 在将消息发送到应用通道之前释放锁。
		rf.mu.Unlock()

		// 将所有消息发送到应用通道。
		for _, messages := range Messages {
			rf.applyChan <- messages
		}
	}
}

//----------------------------------------------leader选举部分------------------------------------------------------------

func (rf *Raft) sendElection() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		// 开启协程对各个节点发起选举
		go func(server int) {
			rf.mu.Lock()
			args := RequestVoteArgs{
				rf.currentTerm + 1,
				rf.me,
				rf.getLastIndex(),
				rf.getLastTerm(),
			}
			reply := RequestVoteReply{}

			rf.printRaft("sendRequestVote之前")
			rf.mu.Unlock()

			res := rf.sendRequestVote(server, &args, &reply)

			if res == true {
				rf.mu.Lock()

				rf.printRaft("sendRequestVote之后")

				// 判断自身是否还是竞选者，且任期不冲突
				if rf.status != Candidate || args.Term < (rf.currentTerm+1) {
					rf.mu.Unlock()
					return
				}

				// 返回者的任期大于args（网络分区原因)进行返回
				if reply.Term > args.Term {
					if (rf.currentTerm + 1) < reply.Term {
						rf.currentTerm = reply.Term
					}
					rf.status = Follower
					rf.votedFor = -1
					rf.voteNum = 0
					rf.persist()
					rf.mu.Unlock()
					return
				}

				// 返回结果正确判断是否大于一半节点同意
				if reply.VoteGranted == true && (rf.currentTerm+1) == args.Term {
					rf.voteNum += 1
					if rf.voteNum >= len(rf.peers)/2+1 {

						//fmt.Printf("[++++elect++++] :Rf[%v] to be leader,term is : %v\n", rf.me, rf.currentTerm)
						rf.status = Leader
						rf.votedFor = -1
						rf.voteNum = 0
						rf.currentTerm += 1 // 在成为leader之后再进行任期自增（不过发送投票信息时的任期为自己当前任期加1，与这里更新后的任期是一致的）
						rf.persist()

						//log.Printf("Peer %d(%d) :leader election success\n", rf.me, rf.currentTerm)
						rf.nextIndex = make([]int, len(rf.peers))
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = rf.getLastIndex() + 1
							//log.Printf("rf.nextIndex[%d]:%d,", i, rf.nextIndex[i])
						}
						//log.Printf("\n")

						rf.matchIndex = make([]int, len(rf.peers))
						rf.matchIndex[rf.me] = rf.getLastIndex()
						rf.votedTimer = time.Now()
						rf.firstNoOp = 0 // todo ? 成为leader之后需要发送一次空操作，确保已经提交的日志不会被覆盖
						rf.firstNoOpSuccessNum = 0
						rf.printRaft("选举成为leader")

						rf.mu.Unlock()

						return
					}
					rf.mu.Unlock()
					return
				}

				rf.mu.Unlock()
				return
			}
		}(i)
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// RequestVote
// example RequestVote RPC handler.
// 个人认为定时刷新的地方应该是别的节点与当前节点在数据上不冲突时才要刷新
// 因为如果不是数据冲突那么定时相当于防止自身去选举的一个心跳
// 如果是因为数据冲突，那么这个节点不用刷新定时是为了当前整个raft能尽快有个正确的leader
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.printRaft("投票之前")

	// 由于网络分区或者是节点crash，导致的任期比接收者还小，直接返回
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	reply.Term = rf.currentTerm

	// 预期的结果:任期大于当前节点，进行重置
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.status = Follower
		rf.votedFor = -1
		rf.voteNum = 0
		rf.persist()
	}

	// If votedFor is null or candidateId, and candidate’s logs is at
	// least as up-to-date as receiver’s logs, grant vote
	if !rf.UpToDate(args.LastLogIndex, args.LastLogTerm) || // 判断日志是否至少要比自己的新
		rf.votedFor != -1 && rf.votedFor != args.CandidateId && args.Term == reply.Term { // paper中的第二个条件votedFor is null
		// 这里的args.Term == reply.Term的理解是：如果args.Term > rf.currentTerm，则肯定会对rf进行重置并投票；如果args.Term == reply.Term(rf.currentTerm)，保证每个任期只投票一次
		// 满足以上两个其中一个都返回false，不给予投票
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	} else {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.votedTimer = time.Now()
		rf.persist()

		rf.printRaft("投票之后")

		return
	}
}

//----------------------------------------------日志增量部分------------------------------------------------------------
func (rf *Raft) leaderAppendEntries() {

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}

		// 开启协程并发的进行日志增量
		go func(server int) {
			rf.mu.Lock()
			if rf.status != Leader {
				rf.mu.Unlock()
				return
			}

			// installSnapshot，如果rf.nextIndex[i]-1小于等于lastIncludeIndex,说明followers的日志小于自身的快照状态，将自己的快照发过去
			// 同时要注意的是比快照还小时，已经算是比较落后
			if rf.nextIndex[server]-1 < rf.lastIncludeIndex {
				go rf.leaderSendSnapShot(server)
				rf.mu.Unlock()
				return
			}

			prevLogIndex, prevLogTerm := rf.getPrevLogInfo(server)
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				LeaderCommit: rf.commitIndex,
			}
			//&& rf.nextIndex[server] > 0
			if rf.getLastIndex() >= rf.nextIndex[server] && rf.firstNoOp == 1 { //  必须在成为leader之后发送空操作给其他节点才能进行日志复制
				//log.Printf("Peer %d(%d) :%d(%d) request vote，term is less my term,do not vote\n", rf.me, rf.currentTerm, args.PrevLogIndex, args.PrevLogTerm)
				//log.Printf("rf.getLastInedx:%d,rf.nextIndex[%d]:%d\n", rf.getLastIndex(), server, rf.nextIndex[server])
				entries := make([]LogEntry, 0)
				entries = append(entries, rf.logs[rf.nextIndex[server]-rf.lastIncludeIndex-1:]...) // 考虑快照rf.nextIndex[server]-rf.lastIncludeIndex-1
				args.Entries = entries
			} else {
				args.Entries = []LogEntry{} // rf.getLastIndex() < rf.nextIndex[server]，则发送心跳信息（后续考虑在日志同步之前先发送一次空日志）
			}
			reply := AppendEntriesReply{}

			rf.printRaft("sendAppendEntries之前")

			rf.mu.Unlock()

			//fmt.Printf("[TIKER-SendHeart-Rf(%v)-To(%v)] args:%+v,curStatus%v\n", rf.me, server, args, rf.status)
			re := rf.sendAppendEntries(server, &args, &reply)

			if re == true {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if rf.status != Leader {
					return
				}

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.status = Follower
					rf.votedFor = -1
					rf.voteNum = 0
					rf.persist()
					rf.votedTimer = time.Now() // 更新为follower之后，需要更新
					return
				}

				rf.printRaft("sendAppendEntries之后")

				if reply.Success {
					if rf.firstNoOp == 0 {
						rf.firstNoOpSuccessNum += 1
						if rf.firstNoOpSuccessNum >= (len(rf.peers)/2 + 1) {
							rf.firstNoOp = 1
						}
					}

					// 参数初始化
					rf.commitIndex = rf.lastIncludeIndex
					//rf.commitIndex = 0
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					//log.Printf("Peer %d(%d):init:rf.nextIndex[%d]:%d\n", rf.me, rf.currentTerm, server, rf.nextIndex[server])

					// 外层遍历下标是否满足,从快照最后开始反向进行（index >= rf.lastIncludeIndex+1）
					for index := rf.getLastIndex(); index > rf.lastIncludeIndex; index-- {
						sum := 0
						for i := 0; i < len(rf.peers); i++ {
							if i == rf.me {
								sum += 1
								continue
							}
							if rf.matchIndex[i] >= index {
								sum += 1
							}
						}

						// 大于一半，且因为是从后往前，一定会大于原本commitIndex(与此同时，还需要判断对应index的日志任期是否与自己的任期相同)
						if sum >= len(rf.peers)/2+1 && rf.restoreLogTerm(index) == rf.currentTerm {
							rf.commitIndex = index
							rf.printRaft("commit成功")
							break
						}

					}
				} else { // 返回为冲突
					// 如果冲突不为-1，则进行更新
					if reply.UpNextIndex != -1 {
						rf.nextIndex[server] = reply.UpNextIndex
						//log.Printf("Peer %d(%d):UpNextIndex:rf.nextIndex[%d]:%d\n", rf.me, rf.currentTerm, server, rf.nextIndex[server])
					}
				}
			}

		}(i)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer fmt.Printf("[	AppendEntries--Return-Rf(%v) 	] arg:%+v, reply:%+v\n", rf.me, args, reply)
	rf.printAppendEntriesArgs(args)

	// Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.UpNextIndex = -1
		return
	}

	reply.Success = true
	reply.Term = args.Term
	reply.UpNextIndex = -1

	rf.status = Follower
	rf.currentTerm = args.Term
	rf.votedFor = -1
	rf.voteNum = 0
	rf.persist()
	rf.votedTimer = time.Now()

	// Reply false if log doesn’t contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	// 自身的快照Index比发过来的prevLogIndex还大，所以返回冲突的下标加1(原因是冲突的下标用来更新nextIndex，nextIndex比Prev大1
	// 返回冲突下标的目的是为了减少RPC请求次数
	if rf.lastIncludeIndex > args.PrevLogIndex {
		reply.Success = false
		reply.UpNextIndex = rf.getLastIndex() + 1
		return
	}

	// 如果自身最后的快照日志比prev小说明中间有缺失日志，such 3、4、5、6、7 返回的开头为6、7，而自身到4，缺失5
	if rf.getLastIndex() < args.PrevLogIndex {
		reply.Success = false
		reply.UpNextIndex = rf.getLastIndex() // todo ? + 1
		//log.Printf("Peer %d(%d):UpNextIndex in AppendEntries:%d\n", rf.me, rf.currentTerm, reply.UpNextIndex)
		return
	} else {
		if rf.restoreLogTerm(args.PrevLogIndex) != args.PrevLogTerm {
			reply.Success = false
			tempTerm := rf.restoreLogTerm(args.PrevLogIndex)
			for index := args.PrevLogIndex; index >= rf.lastIncludeIndex; index-- { // index >= rf.lastIncludeIndex
				if rf.restoreLogTerm(index) != tempTerm {
					reply.UpNextIndex = index + 1
					break
				}
			}
			return
		}
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that follow it (§5.3)
	// Append any new entries not already in the log
	// 进行日志的截取
	rf.logs = append(rf.logs[:args.PrevLogIndex-rf.lastIncludeIndex], args.Entries...) // args.PrevLogIndex-rf.lastIncludeIndex
	rf.persist()

	//If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// commitIndex取leaderCommit与last new entry最小值的原因是，虽然应该更新到leaderCommit，但是new entry的下标更小
	// 则说明日志不存在，更新commit的目的是为了applied log，这样会导致日志日志下标溢出
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
	}

	rf.printRaft("AppendEntries之后")

	return
}

//----------------------------------------------日志压缩(快照）部分---------------------------------------------------------
func (rf *Raft) leaderSendSnapShot(server int) {

	rf.mu.Lock()

	args := InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		rf.lastIncludeIndex,
		rf.lastIncludeTerm,
		rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}

	rf.mu.Unlock()

	res := rf.sendSnapShot(server, &args, &reply)

	if res == true {
		rf.mu.Lock()
		if rf.status != Leader || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}

		// 如果返回的term比自己大说明自身数据已经不合适了
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.status = Follower
			rf.votedFor = -1
			rf.voteNum = 0
			rf.persist()
			rf.votedTimer = time.Now() // 更新为follower之后，需要更新
			rf.mu.Unlock()
			return
		}

		rf.matchIndex[server] = args.LastIncludeIndex
		rf.nextIndex[server] = args.LastIncludeIndex + 1

		rf.mu.Unlock()
		return
	}
}

func (rf *Raft) sendSnapShot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
}

// InstallSnapShot RPC Handler
func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}

	// 更新任期为较大者Max(rf.currentTerm,args.Term)
	rf.currentTerm = args.Term
	reply.Term = args.Term

	rf.status = Follower
	rf.votedFor = -1
	rf.voteNum = 0
	rf.persist()
	rf.votedTimer = time.Now()

	if rf.lastIncludeIndex >= args.LastIncludeIndex {
		rf.mu.Unlock()
		return
	}

	// 将快照后的logs切割，快照前的直接applied
	index := args.LastIncludeIndex
	tempLog := make([]LogEntry, 0)
	//tempLog = append(tempLog, LogEntry{})

	for i := index + 1; i <= rf.getLastIndex(); i++ {
		tempLog = append(tempLog, rf.restoreLog(i))
	}

	rf.lastIncludeTerm = args.LastIncludeTerm
	rf.lastIncludeIndex = args.LastIncludeIndex

	rf.logs = tempLog
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}
	// 更新rf之前保存的快照
	rf.persister.SaveStateAndSnapshot(rf.persistData(), args.Data)

	// 将快照的应用消息添加到通道中，便于上层server获取
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastIncludeTerm,
		SnapshotIndex: rf.lastIncludeIndex,
	}
	rf.mu.Unlock()

	rf.applyChan <- msg
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// index代表是快照apply应用的index,而snapshot代表的是上层service传来的快照字节流，包括了Index之前的数据
// 这个函数的目的是把安装到快照里的日志抛弃，并安装快照数据，同时更新快照下标，属于peers自身主动更新，与leader发送快照不冲突
// 生成快照，从rf.logs的0到index生成快照，index+1到rf.getLastIndex()的日志被保存下来
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果index比rf.lastInculdeIndex小，说明index太小，index对应的日志已经被安装了快照；
	// 如果index比rf.commitIndex大，那么index对应的日志没有被提交，不能进行快照安装
	//fmt.Println("[Snapshot] commintIndex", rf.commitIndex)
	if rf.lastIncludeIndex >= index || index > rf.commitIndex {
		return
	}

	// 1.更新快照日志(将index+1之后的日志保存下来)
	sLogs := make([]LogEntry, 0)
	//sLogs = append(sLogs, LogEntry{})
	for i := index + 1; i <= rf.getLastIndex(); i++ {
		sLogs = append(sLogs, rf.restoreLog(i))
	}

	//fmt.Printf("[Snapshot-Rf(%v)]rf.commitIndex:%v,index:%v\n", rf.me, rf.commitIndex, index)
	// 2.更新快照下标/任期/日志
	if index == rf.getLastIndex()+1 { // todo ? 这个似乎不成立，因为如果index等于rf.getLastIndex()+1，意味着大于rf.commitIndex，与前面的判断自相矛盾
		rf.lastIncludeTerm = rf.getLastTerm()
	} else { // 理论上可以直接获取index对应的日志任期
		rf.lastIncludeTerm = rf.restoreLogTerm(index)
	}

	rf.lastIncludeIndex = index
	rf.logs = sLogs // 将截取的日志重新设置成rf.logs

	// 3.apply了快照就应该重置commitIndex、lastApplied
	if index > rf.commitIndex {
		rf.commitIndex = index // todo ? 这个index理论上如果大于rf.commitIndex，那么不会进行快照的更新
	}
	if index > rf.lastApplied {
		rf.lastApplied = index // 将rf.lastApplied更新为快照日志的index
	}

	// 4.持久化快照信息
	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
}

// CondInstallSnapshot
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyChan.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

//----------------------------------------------持久化（persist)部分---------------------------------------------------------
//
// save Raft's persistent status to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persistData() []byte {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	data := w.Bytes()
	//fmt.Printf("RaftNode[%d] persist starts, currentTerm[%d] voteFor[%d] log[%v]\n", rf.me, rf.currentTerm, rf.votedFor, rf.logs)
	return data
}

func (rf *Raft) persist() {
	data := rf.persistData()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted status.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any status?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var lastIncludeIndex int
	var lastIncludeTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&lastIncludeTerm) != nil {
		fmt.Println("decode error")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastIncludeIndex = lastIncludeIndex
		rf.lastIncludeTerm = lastIncludeTerm
	}
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	//log.Println("GetState_start")
	//log.Printf("%d %d", rf.currentTerm, rf.status)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//log.Println("GetState_end")
	return rf.currentTerm, rf.status == Leader
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's logs. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft logs, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
// 将command添加到日志中
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (2B).
	rf.mu.Lock()

	//log.Printf("start\n")

	if rf.killed() == true {
		rf.mu.Unlock()
		return -1, -1, false
	}

	//log.Printf("rf.status:%d,rf.me:%d\n", rf.status, rf.me)

	if rf.status != Leader || rf.firstNoOp == 0 {
		rf.mu.Unlock()
		return -1, -1, false
	} else {
		index := rf.getLastIndex() + 1
		term := rf.currentTerm
		rf.logs = append(rf.logs, LogEntry{Index: index, Term: term, Command: command})
		rf.persist()

		// 打开文件，如果文件不存在就创建
		//result := FileName + fmt.Sprintf("%d", rf.me) + ".txt"
		//file, _ := os.OpenFile(result, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0644)
		//defer file.Close()
		//fmt.Fprintf(file, "<-------------------------add command------------------->\n")

		rf.printRaft("客户端向leader添加日志")

		rf.mu.Unlock()

		return index, term, true
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}
