package mr

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// 定义了相应的常量，并用iota自增的枚举类型来初始化（0，1，2...）
// 1.对worker状态的描述，是正在执行Map任务还是Reduce，又或者是处于Sleep状态
const (
	Map = iota
	Reduce
	Sleep
)

// 2.对于正在处理的任务的状态描述，是处于正常的Working状态还是处于Timeout状态
const (
	Working = iota
	Timeout
)

// 3.对于Map或者Reduce任务进行描述，表示该任务是处理未执行、正在执行、已完成三种状态之一
const (
	NotStarted = iota
	Processing
	Finished
)

// Task结构体，表示当前正在执行的任务
type Task struct {
	Name      string // 任务名字
	Type      int    // 任务类别
	Status    int    // 任务状态：正常/超时
	mFileName string // 如果是map任务，则记录分配给该任务的文件名字
	rFileName int    //如果是reduce任务，则记录分配给该任务的文件组编号
}

// 一个全局递增变量，作为每个Task的名字，用来区分不同的Task，初始化为0
var taskNumber int = 0

type Coordinator struct {
	// Your definitions here.
	mrecord      map[string]int   // 记录需要map的文件，0：未执行，1：正在执行，2：已完成
	rrecord      map[int]int      // 记录需要reduce的文件，0：未执行，1：正在执行，2：已完成
	reducefile   map[int][]string // 记录中间文件（key:int类型的文件组编号，value:string类型的文件名数组）（注意rrecord.size与reducefile.size大小相等）
	taskmap      map[string]*Task // 任务池，记录当前正在执行的任务
	mcount       int              // 记录已经完成map的任务数量
	rcount       int              // 记录已经完成的reduce的任务数量
	mapFinished  bool             // 标志map任务是否已经完成
	reduceNumber int              // 需要执行的reduce的数量
	mutex        sync.Mutex       // 锁
}

// Your code here -- RPC handlers for the worker to call.
// 进行任务超时处理，对于超时的任务重新更新为未执行的状态
func (c *Coordinator) HandleTimeout(taskName string) {
	// 休眠10秒
	time.Sleep(time.Second * 10)
	// 加锁
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// 获取跟踪的任务，如果被跟踪的任务获取不到说明该任务已经被执行完成了，否则说明该任务超时了
	if t, ok := c.taskmap[taskName]; ok {
		// 将当前任务设置为超时
		t.Status = Timeout

		// 如果任务是map类型
		if t.Type == Map {
			f := t.mFileName
			// 如果该任务是正在执行的，需要重新设置为未执行的状态
			if c.mrecord[f] == Processing {
				c.mrecord[f] = NotStarted
			}
		} else if t.Type == Reduce {
			f := t.rFileName
			// 如果该任务是正在执行的，需要重新设置为未执行的状态
			if c.rrecord[f] == Processing {
				c.rrecord[f] = NotStarted
			}
		}
	}
}

// master针对worker对象上报的请求进行回复
func (c *Coordinator) Report(args *ReportStatusRequest, reply *ReportStatusResponse) error {
	reply.X = 1
	// 加锁
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// 如果任务还在任务池中
	if t, ok := c.taskmap[args.TaskName]; ok {
		flag := t.Status

		// 判断任务当前的状态进行不同的处理

		// 第一种情况：任务超时Timeout了，则直接将任务从任务池中剔除
		if flag == Timeout {
			delete(c.taskmap, args.TaskName)
			return nil
		}

		// 第二种情况：任务没有超时，正在进行Working，根据任务的类型进行不同的处理
		ttype := t.Type
		// 如果是Map任务
		if ttype == Map {
			mf := t.mFileName
			c.mrecord[mf] = Finished
			c.mcount += 1
			// 判断当前所有的Map任务是否都已经完成了
			if c.mcount == len(c.mrecord) {
				c.mapFinished = true
			}
			// 需要将已完成的Map任务根据对应的任务编号添加到reducefile中
			for _, v := range args.FileName {
				index := strings.LastIndex(v, "_")
				num, err := strconv.Atoi(v[index+1:])
				if err != nil {
					log.Fatal(err)
				}
				c.reducefile[num] = append(c.reducefile[num], v)
			}
			// 将该任务从taskMap正在执行的任务中剔除
			delete(c.taskmap, t.Name)
			return nil
		} else if ttype == Reduce { // 如果是Reduce任务
			rf := t.rFileName
			c.rrecord[rf] = Finished
			c.rcount += 1
			delete(c.taskmap, t.Name)
			return nil
		} else {
			log.Fatal("task type is not map and reduce")
		}
	}

	message := fmt.Sprintf("%s task is not int Master record", args.TaskName)
	log.Println(message)
	return nil
}

// worker向master请求任务，master根据请求分配任务给worker
func (c *Coordinator) GetTask(args *GetTaskRequest, reply *GetTaskResponse) error {
	// 加锁
	c.mutex.Lock()
	defer c.mutex.Unlock()

	reply.RFileName = make([]string, 0)
	reply.ReduceNumber = c.reduceNumber
	reply.MFileName = ""
	reply.TaskName = strconv.Itoa(taskNumber)
	taskNumber += 1

	// 如果map任务都已经完成了，则直接分配Reduce任务
	if c.mapFinished {
		for v := range c.rrecord {
			flag := c.rrecord[v]

			// 如果当前任务正在执行或者已完成，则去判断下一个任务
			if flag == Processing || flag == Finished {
				continue
			} else {
				c.rrecord[v] = Processing
				for _, fileName := range c.reducefile[v] {
					reply.RFileName = append(reply.RFileName, fileName)
				}
				reply.TaskType = Reduce
				// 创建一个task任务，并添加到taskmap中
				t := &Task{reply.TaskName, reply.TaskType, Working, "", v}
				c.taskmap[reply.TaskName] = t
				// 启动一个协程，跟踪当前任务,如果当任务超时了就进行相应的处理
				go c.HandleTimeout(reply.TaskName)
				return nil
			}
		}

		// 如果当前没有任务可进行分配，则将状态设置为Sleep
		reply.TaskType = Sleep
		return nil
	} else { // 优先完成Map任务，如果Map任务没有全部完成，则不能开始Reduce任务
		// 分配map任务
		for v, _ := range c.mrecord {
			flag := c.mrecord[v]

			// 如果当前任务正在执行或者已经完成，则直接跳过进行下一个
			if flag == Processing || flag == Finished {
				continue
			} else {
				// 修改记录的状态
				c.mrecord[v] = Processing
				reply.MFileName = v
				reply.TaskType = Map
				// 创建task任务，并添加到taskmap中
				t := &Task{reply.TaskName, reply.TaskType, Working, reply.MFileName, -1}
				c.taskmap[reply.TaskName] = t
				//启动协程，跟踪当前任务，如果当前任务超时了就进行相应的处理
				go c.HandleTimeout(reply.TaskName)
				return nil
			}
		}

		reply.TaskType = Sleep
		return nil
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	// 将对象m注册到远程服务RPC中，以便客户端可以调用这个服务
	rpc.Register(c)
	// 注册了RPC处理程序，以便可以通过HTTP协议来处理客户端请求
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	// 删除之前生成的套接字文件
	os.Remove(sockname)
	// 创建一个Unix域上的套接字，并在该套接字上监听来自客户端的请求
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	// 启动http服务，并监听来自客户端的socket请求
	log.Println("listen successed")
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	// 判断是否已经全部完成
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if c.rcount == c.reduceNumber {
		ret = true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
// files为文件名数组，nReduce用户指定的Reduce的数量
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// 初始化
	c := Coordinator{
		mrecord:      make(map[string]int),
		rrecord:      make(map[int]int),
		reducefile:   make(map[int][]string),
		taskmap:      make(map[string]*Task),
		mcount:       0,
		rcount:       0,
		mapFinished:  false,
		reduceNumber: nReduce,
		mutex:        sync.Mutex{},
	}

	// Your code here.

	for _, f := range files {
		c.mrecord[f] = 0
	}

	for i := 0; i < nReduce; i++ {
		c.rrecord[i] = 0
	}

	c.server()
	return &c
}
