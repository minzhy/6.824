package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// type Node struct {
// 	data    string
// 	version int64

// 	next *Node
// }

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	// 第一个是first，第二个是last，第三个是现在这个node的长度
	// 第一个是first，第二个是现在这个node的长度
	// data        map[string][]interface{}
	// 好像用Node 链表的方式，内存会撑不住！
	// data        map[string][]interface{} // 第一个是first，第二个是现在这个node的长度

	//第一个列表，一共n个位置，每个位置记录每次的信息，第二个列表记录data string，version int64，location int
	data map[string][][]interface{}

	node_length int
	// now_node_length int
	// key_state map[string]string // 第一个位置显示OP，第二个位置显示时间
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	key := args.Key
	if kv.data[key] == nil {
		reply.Value = ""
		kv.mu.Unlock()
		return
	}
	// node, ok := kv.data[key][0].(*Node)
	// if !ok {
	// 	log.Fatal("[Get]::interface")
	// }
	// value := node.data

	for i := 0; i < kv.node_length; i++ {
		if kv.data[key][i] == nil {
			continue
		}
		if kv.data[key][i][2].(int) == 0 {
			reply.Value = kv.data[key][i][0].(string)
		}
	}

	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	key := args.Key
	value := args.Value
	// time_op, _ := time.Parse(timeLayoutStr, args.Time)

	// 第一次put
	if kv.data[key] == nil {
		kv.data[key] = make([][]interface{}, kv.node_length)
		kv.data[key][0] = []interface{}{value, args.Version, int(0)}
		kv.mu.Unlock()
		return
	}

	// 如果开始节点的时间是一样的，那么不需要操作
	for _, item := range kv.data[key] {
		if item == nil || item[2].(int) != 0 {
			continue
		}
		if item[1].(int64) == args.Version {
			reply.Value = item[0].(string)
			kv.mu.Unlock()
			return
		}
	}

	kv.data[key] = make([][]interface{}, kv.node_length)
	kv.data[key][0] = []interface{}{value, args.Version, int(0)}
	// kv.now_node_length = 1
	reply.Value = value
	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	key := args.Key
	value := args.Value
	// time_op, _ := time.Parse(timeLayoutStr, args.Time)

	//append 当put用
	if kv.data[key] == nil {
		kv.data[key] = make([][]interface{}, kv.node_length)
		kv.data[key][0] = []interface{}{value, args.Version, int(0)}
		reply.Value = ""
		kv.mu.Unlock()
		return
	}

	//duplicate
	find_loc, ok := kv.find_ver(key, args.Version)
	// fmt.Println("find_loc", find_loc)d
	if ok {
		if find_loc == kv.node_length-1 {
			reply.Value = ""
			kv.mu.Unlock()
			return
		}
		data, ok := kv.find_loc(key, find_loc+1)
		if ok {
			reply.Value = data
			kv.mu.Unlock()
			return
		} else {
			log.Fatal("error")
		}
	}

	// var find_version *Node = kv.data[key][0].(*Node)
	// for find_version != nil && args.Version != find_version.version {
	// 	find_version = find_version.next
	// }
	// if find_version != nil {
	// 	// 考虑next 为nil的情况，及一开始是append进来的key，那么上一个value就是空
	// 	if find_version.next == nil {
	// 		reply.Value = ""
	// 		kv.mu.Unlock()
	// 		return
	// 	}
	// 	reply.Value = find_version.next.data
	// 	kv.mu.Unlock()
	// 	return
	// }

	// if kv.node_length < kv.data[key][2].(int)+1 {
	// 	var last_node *Node = kv.data[key][0].(*Node)
	// 	var new_end *Node
	// 	for last_node.next != nil {
	// 		last_node = last_node.next
	// 		new_end = last_node
	// 	}
	// 	new_end.next = nil
	// 	last_node = nil
	// } else {
	// 	kv.data[key][1] = kv.data[key][1].(int) + 1
	// }
	// reply.Value = kv.data[key][0].(*Node).data
	// new_node := Node{kv.data[key][0].(*Node).data + value, args.Version, kv.data[key][0].(*Node)}
	// kv.data[key][0] = &new_node

	// not duplicate

	// find origin value
	origin_data := ""
	for idx, item := range kv.data[key] {
		if item != nil {
			if item[2].(int) == 0 {
				origin_data = item[0].(string)
			}
			kv.data[key][idx][2] = item[2].(int) + 1
		}
	}

	// fmt.Println("value", value)
	// fmt.Println("old", origin_data)
	reply.Value = origin_data

	// append
	for idx, item := range kv.data[key] {
		if item == nil {
			kv.data[key][idx] = make([]interface{}, 3)
			kv.data[key][idx][0] = origin_data + value
			kv.data[key][idx][1] = args.Version
			kv.data[key][idx][2] = int(0)
			break
		} else if item != nil && item[2].(int) == kv.node_length {
			kv.data[key][idx][0] = origin_data + value
			kv.data[key][idx][1] = args.Version
			kv.data[key][idx][2] = int(0)
			break
		}
	}

	kv.mu.Unlock()
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	// kv.data = make(map[string]string)
	// kv.key_state = make(map[string]string)
	kv.data = make(map[string][][]interface{})
	kv.node_length = 5
	// kv.now_node_length = 0
	// 每个key的node长度不一样，不能整体定义

	// You may need initialization code here.

	return kv
}

// func (kv *KVServer) show_all(key string) {
// 	st_node, ok := kv.data[key][0].(*Node)
// 	if !ok {
// 		log.Fatal("[show_all]::interface")
// 	}
// 	for st_node != nil {
// 		fmt.Println(st_node.data)
// 		st_node = st_node.next
// 	}
// }

func (kv *KVServer) find_ver(key string, version int64) (loc int, ok bool) {
	// fmt.Println("version", version)
	for _, item := range kv.data[key] {
		// if item != nil {
		// 	fmt.Println(item[1].(int64))
		// }
		if item != nil && item[1].(int64) == version {
			return item[2].(int), true
		}
	}
	return -1, false
}

func (kv *KVServer) find_loc(key string, loc int) (data string, ok bool) {
	for _, item := range kv.data[key] {
		if item != nil && item[2].(int) == loc {
			return item[0].(string), true
		}
	}
	return "", false
}
