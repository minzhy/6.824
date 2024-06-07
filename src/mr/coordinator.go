package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type State int
type Type int

const (
	Undone      State = 0
	Progressing State = 1
	Done        State = 2

	Unknown Type = 0
	Map     Type = 1
	Reduce  Type = 2
)

type Coordinator struct {
	// Your definitions here.
	WorkerId2state  map[int]int
	WorkerId2live   map[int]bool
	WorkerId2TaskId map[int]int
	WorkerId2Ticker map[int]*time.Ticker
	TaskId2Info     map[int][]int // list 的第一个位置是type，第二个位置是state
	TaskId2FileName map[int]string

	TaskNumber int
	NReduce    int
	NMap       int
	TaskFinish bool
	MapFinish  bool

	mu sync.Mutex
	mp sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) FindState(t Type, s State) (assignId int, mrType Type) {
	assignId = -1
	mrType = Unknown
	for id, item := range c.TaskId2Info {
		if item[0] == int(t) && item[1] == int(s) {
			assignId = id
			mrType = t
			c.mp.Lock()
			c.TaskId2Info[assignId][1] = int(Progressing)
			c.mp.Unlock()
			break
		}
	}
	return
}

func (c *Coordinator) CreateWorker(args *CreateWorkerArgs, reply *CreateWorkerReply) error {
	workerId := args.WorkerId
	reply.Res = false
	c.mp.Lock()
	c.WorkerId2state[workerId] = int(Progressing)
	ticker := time.NewTicker(8 * time.Second)
	c.WorkerId2live[workerId] = true
	c.WorkerId2Ticker[workerId] = ticker
	c.mp.Unlock()

	go func(t *time.Ticker, workerId int) {
		for {
			select {
			case <-t.C:
				c.mp.Lock()
				taskId := c.WorkerId2TaskId[workerId]
				// if taskId != -1 {
				// fmt.Println("[CreateWorker]::workerId", workerId)
				if c.WorkerId2live[workerId] {
					c.WorkerId2live[workerId] = false
				} else {
					delete(c.WorkerId2live, workerId)
					delete(c.WorkerId2state, workerId)
					// fmt.Println("[CreateWorker]::delete", workerId, "taskId", taskId, "time", time.Now())
					c.TaskId2Info[taskId][1] = int(Undone)
					c.mp.Unlock()
					ticker.Stop()
					return
				}
				// }
				c.mp.Unlock()
			}
		}
	}(ticker, workerId)

	reply.Res = true

	return nil
}

func (c *Coordinator) AssignWorker(args *AssignArgs, reply *AssignReply) error {
	workerId := args.WorkerId
	assignId := -1
	mrType := Unknown

	// fmt.Println("[assign worker]::in assign workerId", workerId)
	// map
	c.mu.Lock()
	// fmt.Println("[assign worker]::workerId", workerId, "hold the key")
	assignId, mrType = c.FindState(Map, Undone)
	if mrType == Unknown && assignId == -1 {
		if !c.MapFinish {
			// 这里就是要保证map和reduce的顺序，map要完全结束了，才能reduce，如果map了一半，就reduce，就会导致读取出来的数据不全!
			// 这里可以用通道写等待
			// time.Sleep(1000)
			// 不能在这里等待，如果map的最后一个task 分配的worker失败了，其他所有的worker都会卡在这里，让程序卡住的！
			// 循环的部分，应该在worker端实现
			reply.Type = int(mrType)
			reply.TaskId = assignId
			c.mu.Unlock() // 这个地方漏了，就会导致拿了key，但是没有释放的情况！！！！[assign worker]:
			return nil
		}
		// reduce
		assignId, mrType = c.FindState(Reduce, Undone)
	}
	c.mu.Unlock()
	// fmt.Println("[assign worker]::workerId", workerId, "release the key")

	// fmt.Println("[assign worker]::assign task:", assignId, "workerId", workerId)
	reply.Type = int(mrType)
	reply.TaskId = assignId

	if assignId == -1 {
		// c.TaskFinish = true
		// assignId == -1表示，已经没有task是undone的状态，但有可能有task是progressing
		// 需要在TaskDone中判断是否WorkerFinish，否则coordinate可能会比worker先关闭，逻辑上讲分配任务的时候，不应该结束worker
		// 其实可以在DoneTask的时候，就结束worker和coordinator，所以，根本不会到这里，不可能assignId==-1
		// 但是如果是多个worker同时执行同一个task的话，倒是有可能。
		// for idx, item := range c.TaskId2Info {
		// 	fmt.Println(idx, item[1])
		// }
		return nil
	}

	c.mp.Lock()
	c.WorkerId2TaskId[workerId] = assignId
	c.mp.Unlock()

	if mrType == Map {
		reply.FileName = c.TaskId2FileName[assignId]
		reply.NReduce = c.NReduce
	} else if mrType == Reduce {
		reply.ReduceId = assignId - c.NMap
	}

	return nil
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	taskId := args.TaskId
	workerId := args.WorkerId

	// taskId == -1 可能是因为上面assign 的时候，给的taskId是-1，多个worker一起与一个coordinator通讯发生的问题
	if taskId != -1 {
		c.mp.Lock()
		fmt.Println("[TaskDone]::taskId", taskId, c.TaskId2FileName[taskId], "state", c.TaskId2Info[taskId][1], "mapfinish", c.MapFinish)
		c.TaskId2Info[taskId][1] = int(Done)
		c.WorkerId2TaskId[workerId] = -1
		c.mp.Unlock()
	}

	// 不能直接用MapFinish作为循环，因为如果这个时候有进程进来assign task，判断mapfinish是否完成，可能会直接分配reduce的task
	// c.TaskFinish = true
	// c.MapFinish = true
	// for _, item := range c.TaskId2Info {
	// 	c.TaskFinish = c.TaskFinish && (item[1] == int(Done)) // 如果一个task 是done或者是progress
	// 	if item[0] == int(Map) {
	// 		c.MapFinish = (c.MapFinish && (item[1] == int(Done)))
	// 	}
	// }
	tmpTaskFinish := true
	tmpMapFinish := true
	for _, item := range c.TaskId2Info {
		tmpTaskFinish = tmpTaskFinish && (item[1] == int(Done)) // 如果一个task 是done或者是progress
		if item[0] == int(Map) {
			tmpMapFinish = (tmpMapFinish && (item[1] == int(Done)))
		}
	}

	// 这里也需要一个锁，保证mapfinish 变成true的过程，是原子过程，不然就容易出错
	c.mu.Lock()
	c.MapFinish = tmpMapFinish
	c.TaskFinish = tmpTaskFinish
	c.mu.Unlock()

	// // fmt.Println("[TaskDone]::workerId", workerId, "mapfinish", c.MapFinish)
	// for idx, item := range c.TaskId2Info {
	// 	if (item[1] == int(Progressing) || item[1] == int(Undone)) && item[0] == int(Map) {
	// 		fmt.Println(idx, item[1])
	// 	}
	// 	// fmt.Println(idx, item[1])
	// }

	if c.TaskFinish {
		// 回收所有资源
		c.mp.Lock()
		c.WorkerId2state[args.WorkerId] = int(Done)
		reply.EndWorker = true
		c.WorkerId2Ticker[workerId].Stop()
		delete(c.WorkerId2TaskId, workerId)
		delete(c.WorkerId2live, workerId)
		c.mp.Unlock()
	}

	return nil
}

func (c *Coordinator) WorkerTick(args *WorkerTickArgs, reply *WorkerTickReply) error {
	// 这个error的返回不能漏了，不然call不上的
	workerId := args.WorkerId
	c.mp.Lock()
	c.WorkerId2live[workerId] = true
	c.mp.Unlock()
	reply.Res = true
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// ret := c.TaskFinish // 不能这样，要保证所有的worker都结束才行
	workerEnd := true
	for _, item := range c.WorkerId2state {
		workerEnd = workerEnd && (item == int(Done))
	}

	// Your code here.

	return c.TaskFinish && workerEnd
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.TaskId2Info = make(map[int][]int)
	c.TaskId2FileName = make(map[int]string)
	c.WorkerId2TaskId = make(map[int]int)
	c.WorkerId2live = make(map[int]bool)
	c.WorkerId2state = make(map[int]int)
	c.WorkerId2Ticker = make(map[int]*time.Ticker)
	nMap := len(files)
	// 在map结束之前其实是不知道reduce task的个数的！！！
	// nReduce其实是给的进程数量但是不是task的个数，reduce 的个数应该是map生成的中间文件的数量
	// 其实不用，可以多创建几个文件夹，coordinate就创建好文件夹，这样就可以了。要不然nreduce就没有意义了
	for i := 0; i < nMap+nReduce; i++ {
		if i < nMap {
			c.TaskId2Info[i] = []int{1, 0}
			c.TaskId2FileName[i] = files[i]
		} else {
			c.TaskId2Info[i] = []int{2, 0}
		}
	}
	c.TaskFinish = false
	c.MapFinish = false

	c.TaskNumber = nMap + nReduce
	c.NMap = nMap
	c.NReduce = nReduce
	for i := 0; i < nReduce; i++ {
		filename := "tmp-mr-out-" + strconv.Itoa(i)
		file, err := os.Create(filename)
		if err != nil {
			fmt.Println("[coordinator::MakeCoordinator] file create fail", err)
		}
		file.Close()
	}
	c.server()
	return &c
}
