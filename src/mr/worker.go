package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	// create worker
	WorkerId := os.Getpid()
	createargs := CreateWorkerArgs{}
	createreply := CreateWorkerReply{}
	createargs.WorkerId = WorkerId
	ok := call("Coordinator.CreateWorker", &createargs, &createreply)
	if !ok || !createreply.Res {
		fmt.Printf("[Worker]::call create failed or create false!\n")
	}

	// 在这个地方加不行，主进程exit了，但是计数器还在工作，不影响啊。所以无法判断是否还在进行计算
	// workertickargs := WorkerTickArgs{}
	// workertickreply := WorkerTickReply{}
	// workertickargs.WorkerId = WorkerId
	// ticker := time.NewTicker(1 * time.Second)
	// defer ticker.Stop()
	// go func(t *time.Ticker) {
	// 	for {
	// 		select {
	// 		case <-t.C:
	// 			ok := call("Coordinator.WorkerTick", &workertickargs, &workertickreply)
	// 			if !ok || !workertickreply.Res {
	// 				// fmt.Printf("[Worker]::call tick failed!\n")
	// 				log.Fatal("[Worker]::call tick failed!\n")
	// 			}
	// 		}
	// 	}
	// }(ticker)

	for {
		// assignargs := AssignArgs{}
		// assignargs.WorkerId = WorkerId
		// assignreply := AssignReply{}
		// ok := call("Coordinator.AssignWorker", &assignargs, &assignreply)
		// if !ok {
		// 	fmt.Printf("[Worker]::call assign failed!\n")
		// }

		// taskId := assignreply.TaskId
		// doneargs := TaskDoneArgs{}
		// doneargs.TaskId = taskId
		// doneargs.WorkerId = WorkerId
		// donereply := TaskDoneReply{}
		// if taskId == -1 {
		// 	// 多个worker并发处理的时候，这里就有可能进来，因为只有一个进程会执行下面那个CallTaskDone
		// 	doneargs.TaskId = -1
		// 	doneargs.WorkerId = WorkerId
		// 	for {
		// 		ok := call("Coordinator.TaskDone", &doneargs, &donereply)
		// 		if !ok {
		// 			fmt.Printf("[Worker]::call taskdone failed!\n")
		// 		}
		// 		time.Sleep(1000)
		// 		if donereply.EndWorker {
		// 			return
		// 		}
		// 	}
		// }

		assignargs := AssignArgs{}
		assignreply := AssignReply{}
		doneargs := TaskDoneArgs{}
		donereply := TaskDoneReply{}
		for {
			// 只要存活，就要tick
			workertickargs := WorkerTickArgs{}
			workertickreply := WorkerTickReply{}
			workertickargs.WorkerId = WorkerId
			ok := call("Coordinator.WorkerTick", &workertickargs, &workertickreply)
			if !ok || !workertickreply.Res {
				// fmt.Printf("[Worker]::call tick failed!\n")
				log.Fatal("[Worker]::call tick failed!\n")
			}
			// fmt.Println("[Worker]::finish tick task", WorkerId)

			// assign task to worker
			assignargs.WorkerId = WorkerId
			ok = call("Coordinator.AssignWorker", &assignargs, &assignreply)
			if !ok {
				log.Fatal("[Worker]::call assign failed!\n")
			}
			// fmt.Println("[Worker]::assign task", assignreply.TaskId, "workerId", WorkerId)

			taskId := assignreply.TaskId
			if taskId != -1 {
				break
			}

			// taskId == -1，则表示无法正确分配任务，所以就是所有任务完成，或者是所有任务progressing。
			doneargs.TaskId = -1
			doneargs.WorkerId = WorkerId
			ok = call("Coordinator.TaskDone", &doneargs, &donereply)
			if !ok {
				fmt.Printf("[Worker]::call taskdone failed!\n")
			}
			time.Sleep(1000)
			if donereply.EndWorker {
				return
			}
		}
		tmpname := "tmp-mr-out-"
		oname := "mr-out-"
		if assignreply.Type == int(Map) {
			// read file content
			filename := assignreply.FileName
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()

			// fmt.Println("[Worker]::start map", assignreply.TaskId, "workerId", WorkerId)
			kva := mapf(filename, string(content))
			// fmt.Println("[Worker]::done map", assignreply.TaskId, "workerId", WorkerId)
			for _, item := range kva {
				num := ihash(item.Key)
				idx := num % assignreply.NReduce
				outputname := tmpname + strconv.Itoa(idx)
				file, err = os.OpenFile(outputname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666) //打开文件
				if err != nil {
					fmt.Println("file open fail", err)
					return
				}
				fmt.Fprintf(file, "%v %v\n", item.Key, item.Value)
				file.Close()
			}
		} else {
			reduceId := assignreply.ReduceId
			inputname := tmpname + strconv.Itoa(reduceId)
			file, err := os.Open(inputname)
			if err != nil {
				log.Fatalf("cannot open %v", inputname)
			}

			// 下面那种读取方式都可以，多线程并行，并不会影响读取缓冲区
			intermediate := []KeyValue{}
			scanner := bufio.NewScanner(file)
			// defer outfile.Close()
			// content, err := ioutil.ReadAll(file)
			for scanner.Scan() {
				line := scanner.Text() // 这里直接是把后面的'\n'去掉了
				words := strings.Split(line, " ")
				// fmt.Print(words[1])
				intermediate = append(intermediate, KeyValue{words[0], words[1]})
			}

			// content, _ := ioutil.ReadAll(file)
			// lines := strings.Split(string(content), "\n")
			// for _, line := range lines {
			// 	words := strings.Split(line, " ")
			// 	if len(words) <= 1 {
			// 		break
			// 	}
			// 	intermediate = append(intermediate, KeyValue{words[0], words[1]})
			// }

			// dec := json.NewDecoder(file)
			// for {
			// 	var kv KeyValue
			// 	if err := dec.Decode(&kv); err != nil {
			// 		break
			// 	}
			// 	fmt.Println(kv.Key, kv.Value)
			// 	intermediate = append(intermediate, kv)
			// }
			// file.Close()
			file.Close()

			sort.Sort(ByKey(intermediate))

			// // checkfile test
			// checkname := "check-mr-out-" + strconv.Itoa(reduceId)
			// checkfile, err := os.Create(checkname)
			// if err != nil {
			// 	log.Fatalf("cannot create %v", checkname)
			// }
			// for _, v := range intermediate {
			// 	fmt.Fprintf(checkfile, "%v %v\n", v.Key, v.Value)
			// }
			// checkfile.Close()

			var res []KeyValue
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				res = append(res, KeyValue{intermediate[i].Key, output})
				// fmt.Fprintf(outfile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			// 其实这里在写进文件的时候，最好去查看一下，有没有worker已经做好了这件事情。
			// 有可能这个worker因为工作太慢，被认为是crash了，而任务已经被其他worker做掉了，这时候，这个worker就可以下线了，当作已经crash了
			// 但是我这个任务，其实可以大致估计每个task最长多长时间，所以不存在这种情况
			outputname := oname + strconv.Itoa(reduceId)
			outfile, err := os.Create(outputname)
			if err != nil {
				log.Fatalf("cannot create %v", outputname)
			}
			for _, v := range res {
				fmt.Fprintf(outfile, "%v %v\n", v.Key, v.Value)
			}

			outfile.Close()
			os.Remove(inputname)
		}
		doneargs.TaskId = assignreply.TaskId
		doneargs.WorkerId = WorkerId
		ok = call("Coordinator.TaskDone", &doneargs, &donereply)
		if !ok {
			fmt.Printf("[Worker]::call taskdone failed!\n")
		}
		if donereply.EndWorker {
			break
		}
	}

	// defer func() {
	// 	fmt.Println("haha")
	// 	if r := recover(); r != nil {
	// 		// 处理panic，例如记录日志
	// 		fmt.Println("Recovered in worker")
	// 	}
	// }()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
