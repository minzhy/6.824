package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int //这个index必须与start函数返回的index值一样，否则检测不到，代码实现的idx与论文中一致，从1开始

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int32

const LeakBeat = 3

const (
	follower State = iota
	candidate
	leader
)

var heartBeatTime time.Duration = 150 * time.Millisecond
var checkLeaderTime time.Duration = 300 * time.Millisecond
var waitTimeVote time.Duration = 30 * time.Millisecond
var waitTimeCheck time.Duration = 5 * time.Millisecond
var waitTimeCommit time.Duration = 5 * time.Millisecond

// var heartBeatChannel = make(chan bool)

type LogEntry struct {
	Log   interface{}
	Term  int64
	Index int64
	// index 不需要显式记录，其实只用查看Raft中Log的长度就知道了！
	// 用Term和Index都为-1的LogEntry表示空的LogEntry
}

type AppendEntriesChan struct {
	Aep     AppendEntriesReply
	PeerNum int64
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu                 sync.Mutex // Lock to protect shared access to this peer's state
	voteForMu          sync.Mutex
	commitMu           sync.Mutex
	leaderLogMu        sync.Mutex
	consistent_checkMu sync.Mutex
	peers              []*labrpc.ClientEnd // RPC end points of all peers
	persister          *Persister          // Object to hold this peer's persisted state // 断电之后，一定要保存的（一定不能缺失的东西
	me                 int                 // this peer's index into peers[]
	dead               int32               // set by Kill()
	apply_msg          chan ApplyMsg

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 去看test，这个Raft相当于每个peer的结构，test的时候，会创建n个peer，也就是这个结构体，每个peer里面，保存了别人的地址peers
	// Temporary persistent state
	CurrentTerm int64
	VotedFor    int64
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int64
	lastApplied int64
	serverState State

	// Volatile state on leaders
	nextIndex []int64
	// initMatchIndex []bool
	matchIndex []int64
	IndexMu    []sync.Mutex

	// my add
	// collectVotes           int64 // 这个东西设置为临时变量就行了，为什么要设置成raft的变量呢，这样容易导致，不同voteterm的得票相加
	findLeader             int
	leaderAlive            int
	majorityNum            int
	isStartConsistentCheck map[int64]bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	term = int(rf.CurrentTerm)
	isleader = (rf.serverState == leader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	// tips：crash了的机器重启的时候，要从之前applied的位置重新开始，不能将前面所有的log全部重新来一遍
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.commitIndex)
	e.Encode(rf.lastApplied)
	e.Encode(rf.log)
	// e.Encode(rf.isStartConsistentCheck)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	Debug(dInfo, "S%v readPersist", rf.me)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var current_term int64
	var voted_for int64
	var commitIndex int64
	var lastApplied int64
	var log []LogEntry
	if d.Decode(&current_term) != nil {
		Debug(dError, "S%v current_term error", rf.me)
	} else {
		rf.CurrentTerm = current_term
	}

	if d.Decode(&voted_for) != nil {
		Debug(dError, "S%v voted_for error", rf.me)
	} else {
		rf.VotedFor = voted_for
	}
	if d.Decode(&commitIndex) != nil || d.Decode(&lastApplied) != nil {
		Debug(dError, "S%v commitIndex lastApplied", rf.me)
	} else {
		rf.commitIndex = commitIndex
		rf.lastApplied = lastApplied
	}

	if d.Decode(&log) != nil {
		Debug(dError, "S%v log", rf.me)
	} else {
		rf.log = log
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int64
	CandidateId  int64
	LastLogIndex int64
	LastLogTerm  int64
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int64
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	term := args.Term
	candidateId := args.CandidateId
	lastLogIndex := args.LastLogIndex
	lastLogTerm := args.LastLogTerm

	if term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		Debug(dVote, "S%v deny vote to %v term:%v, term < rf.currentTerm", rf.me, candidateId, term)
		return
	} else if term > rf.CurrentTerm {
		// term 增加了重新获得投票资格
		rf.voteForMu.Lock()
		rf.CurrentTerm = term
		rf.serverState = follower
		num := len(rf.log)
		if num == 0 || (lastLogTerm > rf.log[num-1].Term) || (lastLogTerm == rf.log[num-1].Term && lastLogIndex >= rf.log[num-1].Index) {
			rf.VotedFor = candidateId
			reply.Term = rf.CurrentTerm
			reply.VoteGranted = true
			rf.leaderAlive = 1
			Debug(dVote, "S%v vote to %v term:%v, update term", rf.me, candidateId, term)
		} else {
			rf.VotedFor = -1
			reply.Term = rf.CurrentTerm
			reply.VoteGranted = false
			Debug(dVote, "S%v deny vote to %v term:%v, last log index and term", rf.me, candidateId, term)
		}
		rf.persist()
		rf.voteForMu.Unlock()
		return
	} else if term == rf.CurrentTerm {
		rf.voteForMu.Lock()
		if rf.VotedFor == -1 {
			num := len(rf.log)
			if num == 0 || (lastLogTerm > rf.log[num-1].Term) || (lastLogTerm == rf.log[num-1].Term && lastLogIndex >= rf.log[num-1].Index) {
				rf.VotedFor = candidateId
				reply.Term = rf.CurrentTerm
				reply.VoteGranted = true
				rf.leaderAlive = 1
				Debug(dVote, "S%v vote to %v term:%v, not update term", rf.me, candidateId, term)
			} else {
				rf.VotedFor = -1
				reply.Term = rf.CurrentTerm
				reply.VoteGranted = false
				Debug(dVote, "S%v deny vote to %v term:%v, last log index and term", rf.me, candidateId, term)
			}
		} else {
			reply.Term = rf.CurrentTerm
			reply.VoteGranted = false
			Debug(dVote, "S%v deny vote to %v term:%v, already vote", rf.me, candidateId, term)
		}
		rf.persist()
		rf.voteForMu.Unlock()
	}

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, requestVoteChan chan struct {
	ok   bool
	peer int64
	r    RequestVoteReply
}) bool {
	// 这个结构根本没有用，如果要记录时间，必须得在go的函数之外记录，这样记录的话，哪怕时间到了，也会卡在ok:=这个位置！！！没办法到下面的select
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	requestVoteChan <- struct {
		ok   bool
		peer int64
		r    RequestVoteReply
	}{ok, int64(server), *reply}

	return true
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// 这个函数是直接返回的
func (rf *Raft) Start(command interface{}) (int, int, bool) { //(index,term,isleader)
	// Your code here (3B).
	if rf.serverState != leader {
		return -1, int(rf.CurrentTerm), false
	}

	rf.leaderLogMu.Lock()
	log_idx := len(rf.log) + 1 // 刚刚加入的log的index值
	log_num := len(rf.log)

	// leader的话，首先是自己先存
	log_entry := LogEntry{command, rf.CurrentTerm, int64(log_idx)}
	rf.log = append(rf.log, log_entry)
	rf.persist()
	Debug(dLog, "S%v log.cmd:%v log.term:%v log.idx:%v [Start]", rf.me, command, rf.CurrentTerm, log_idx)
	rf.leaderLogMu.Unlock()

	// 发送给其他peers
	aea := AppendEntriesArgs{}
	aea.Term = rf.CurrentTerm
	aea.LeaderId = int64(rf.me)
	aea.LogEntries = make([]LogEntry, 0)
	aea.LogEntries = append(aea.LogEntries, log_entry)
	// aea.LeaderCommit = rf.commitIndex
	if log_num == 0 {
		aea.PrevLogIndex = -1
		aea.PrevLogTerm = -1
	} else {
		aea.PrevLogIndex = rf.log[log_num-1].Index
		aea.PrevLogTerm = rf.log[log_num-1].Term
	}
	var sendEntry chan struct {
		ok   bool
		peer int64
		r    AppendEntriesReply
	} = make(chan struct {
		ok   bool
		peer int64
		r    AppendEntriesReply
	})
	// 发送RPC
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		aep := AppendEntriesReply{}
		go rf.sendAppendEntries(idx, &aea, &aep, sendEntry)
	}

	// 处理RPC
	go func() {
		var success_num = 1
		var set map[int64]bool = make(map[int64]bool)
		is_success := false
		for out := range sendEntry {
			set[out.peer] = true // 由于上面就send了一次，这里也不用先判断out.peer是否已经在set里面了，或者out.peer已经处理过了
			if out.ok {
				if out.r.Term > rf.CurrentTerm {
					// 有可能leader disconnect了，当他终于连上外面的server，发现自己的term过期了
					rf.serverState = follower
					rf.CurrentTerm = out.r.Term
					rf.VotedFor = -1
					rf.persist()
				}
				// 这里就处理好success和fail对leader的影响
				idx := out.peer
				if !out.r.Success {
					if out.r.XTerm == -1 && out.r.XIndex == -1 {
						break
					}
					find_term := out.r.XTerm
					var isfind bool = false
					rf.IndexMu[idx].Lock()
					for i := len(rf.log) - 1; i >= 0; i-- {
						if find_term == rf.log[i].Term {
							rf.nextIndex[idx] = int64(i + 1)
							isfind = true
							break
						}
					}
					if isfind == false && out.r.XTerm == -1 {
						rf.nextIndex[idx] = out.r.XIndex + 1
					} else if isfind == false && out.r.XTerm != -1 {
						rf.nextIndex[idx] = out.r.XIndex
					}
					rf.IndexMu[idx].Unlock()
					rf.consistent_checkMu.Lock()
					if rf.isStartConsistentCheck[int64(idx)] == false {
						go rf.consistent_check(int64(idx), aea.Term)
						rf.isStartConsistentCheck[int64(idx)] = true
					}
					rf.consistent_checkMu.Unlock()
				} else {
					rf.IndexMu[idx].Lock()
					if rf.matchIndex[idx] < aea.LogEntries[0].Index {
						rf.matchIndex[idx] = aea.LogEntries[0].Index
					}
					rf.nextIndex[idx] = rf.matchIndex[idx] + 1
					rf.IndexMu[idx].Unlock()
					success_num++
				}
			}
			if success_num >= rf.majorityNum && !is_success {
				if rf.commitIndex < int64(log_idx) {
					rf.commitIndex = int64(log_idx)
				}
				is_success = true
			}
			if len(set) == len(rf.peers)-1 {
				close(sendEntry)
				break
			}
		}
	}()

	return log_idx, int(rf.CurrentTerm), rf.serverState == leader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.

	// // 死过一次的，必须要初始化一下
	// rf.serverState = follower
	// rf.CurrentTerm = 0
	// rf.VotedFor = -1
	// rf.collectVotes = 0
	// rf.findLeader = -1
	// rf.leaderAlive = false

	// fmt.Println("[Kill]::id", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// if vote leader
		if rf.leaderAlive <= 0 {
			rf.serverState = candidate

			var voteTerm int64
			rf.voteForMu.Lock()
			// 这里不需要初始化了，相当于直接初始化了
			// tips:这里需要保存一个voteTerm变量，相当于接下来的vote，都是针对这个term的vote。不能直接用CurrentTerm来代替
			// 如果用CurrentTerm直接代替的话，有可能这里处理完，收到request vote，改变了CurrentTerm的值，相当于这个机器在CurrentTerm投了两票
			// 这里的vote leader，实际上是vote 这个term下的leader！term在这个过程中，不能改变
			rf.CurrentTerm++
			voteTerm = rf.CurrentTerm
			rf.VotedFor = int64(rf.me)
			// atomic.AddInt64(&rf.collectVotes, 1)
			collectVotes := 1
			rf.persist()
			rf.voteForMu.Unlock()

			Debug(dVote, "S%v start elect leader term:%v, leaderAlive%v", rf.me, voteTerm, rf.leaderAlive)

			var requestVoteChan chan struct {
				ok   bool
				peer int64
				r    RequestVoteReply
			} = make(chan struct {
				ok   bool
				peer int64
				r    RequestVoteReply
			}, len(rf.peers)-1)
			timeout := time.NewTimer(waitTimeVote)
			// sendRequestVote
			for id := range rf.peers {
				if id == rf.me {
					continue
				}
				rva := RequestVoteArgs{}
				rvr := RequestVoteReply{}
				rva.Term = voteTerm
				rva.CandidateId = int64(rf.me)
				if len(rf.log) == 0 {
					rva.LastLogIndex = -1
					rva.LastLogTerm = -1
				} else {
					rva.LastLogIndex = rf.log[len(rf.log)-1].Index
					rva.LastLogTerm = rf.log[len(rf.log)-1].Term
				}

				// timeout := time.NewTimer(voteWaitTime) //timer和ticker区别在于，ticker是循环，但是timer是只记录一次时间，而不是每次时间过后，需要reset重新设置时间

				go rf.sendRequestVote(id, &rva, &rvr, requestVoteChan)

				// 原先这种方式在3C的时候，由于网络不通畅，wait时间太短，等不到回复，wait时间太长。
				// 如果有的回复是断开的，那么总体的等待时间就过长，导致各自只给各自投票。votefor都被占用了
			}
			// recieve request
			var recieveVoteChan chan bool = make(chan bool)
			go func() {
				var issend bool = false
				var set map[int64]bool = make(map[int64]bool)
				for out := range requestVoteChan {
					set[out.peer] = true
					Debug(dVote, "S%v get reply from %v, ok:%v [ticker]", rf.me, out.peer, out.ok)
					if out.ok {
						term := out.r.Term
						if term <= voteTerm && out.r.VoteGranted {
							collectVotes += 1
							Debug(dVote, "S%v get voted from %v in voteTerm:%v currentTerm:%v [ticker]", rf.me, out.peer, voteTerm, rf.CurrentTerm)
						} else if term > voteTerm {
							if term > rf.CurrentTerm {
								rf.voteForMu.Lock()
								rf.CurrentTerm = term
								rf.findLeader = -1
								rf.VotedFor = -1
								rf.voteForMu.Unlock()
							}
							collectVotes = 0
							rf.serverState = follower
							return
						}
						if collectVotes >= rf.majorityNum && !issend {
							recieveVoteChan <- true
							issend = true
							close(recieveVoteChan)
						}
						if len(set) == len(rf.peers)-1 {
							close(requestVoteChan)
							return
						}
					}
				}
			}()
			select {
			case <-timeout.C:
				Debug(dVote, "S%v vote timeout in term:%v", rf.me, voteTerm)
				timeout.Stop()
			case <-recieveVoteChan:
				timeout.Stop()
			}
			Debug(dVote, "S%v collectVotes:%v voteTerm:%v [ticker]", rf.me, collectVotes, voteTerm)
			// win election
			// 想要成为leader，必须得经过win election，所以，只有这里能让一个server成为leader！
			if collectVotes >= rf.majorityNum && rf.CurrentTerm == voteTerm {
				// heartbeat
				// 当选的是voteTerm的leader
				go rf.start_heartbeat(voteTerm)
				rf.serverState = leader
				rf.leaderAlive = LeakBeat
				rf.findLeader = rf.me
				// 成为leader后，需要初始化一些变量	nextIndex  []int64 和 matchIndex []int64
				// 假设append一个已存在的log的结果为true，幂等
				// 保证matchIndex是单调增的
				rf.matchIndex = make([]int64, len(rf.peers))
				// rf.initMatchIndex = make([]bool, len(rf.peers))
				rf.nextIndex = make([]int64, len(rf.peers))
				rf.leaderLogMu.Lock()
				for idx := range rf.peers {
					// matchIndex 逻辑是已经commit的log的位置，所以当leader知道peer commit的位置的时候，需要调整
					// nextIndex 逻辑是最大的可能复制的log的位置，所以当leader log的长度变化的时候需要考虑变化
					if idx == rf.me {
						rf.matchIndex[idx] = rf.commitIndex
						// rf.initMatchIndex[idx] = true
						rf.nextIndex[idx] = int64(len(rf.log)) + 1
						continue
					}
					// rf.consistent_checkMu.Lock()
					// if rf.isStartConsistentCheck[int64(idx)] == false {
					// 	go rf.consistent_check(int64(idx), voteTerm)
					// 	rf.isStartConsistentCheck[int64(idx)] = true
					// }
					// rf.consistent_checkMu.Unlock()
					rf.matchIndex[idx] = 0
					// rf.initMatchIndex[idx] = false
					rf.nextIndex[idx] = int64(len(rf.log)) + 1
					Debug(dInfo, "S%v store nextIndex[%v]=%v [ticker]", rf.me, idx, rf.nextIndex[idx])
				}
				rf.leaderLogMu.Unlock()
				// rf.Start("minzhy" + strconv.Itoa(int(rf.CurrentTerm))) // 3B lab这里要去掉，因为3 B会测试lab的index序列号，没预期会增加一个
			} else {
				rf.serverState = follower
			}
			// EndVote:
			// DPrintf("[ticker]::vote collection vote:%v", rf.collectVotes, "me", rf.me, "term", voteTerm)
			// clear state
		}
		// 看看log里面都是什么，为什么选不出leader
		if id := len(rf.log) - 1; id != -1 {
			Debug(dInfo, "S%v last log.cmd:%v log.index:%v log.term:%v [ticker]", rf.me, rf.log[id].Log, rf.log[id].Index, rf.log[id].Term)
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) start_heartbeat(voteTerm int64) {
	for {
		if rf.serverState == leader && !rf.killed() && voteTerm == rf.CurrentTerm {
			var sendHeartBeatChan chan struct {
				ok   bool
				peer int64
				r    AppendEntriesReply
			} = make(chan struct {
				ok   bool
				peer int64
				r    AppendEntriesReply
			})
			for id := range rf.peers {
				if id == rf.me {
					continue
				}
				// 这里需要判断log的长度，所以需要枷锁
				rf.leaderLogMu.Lock()
				aea := AppendEntriesArgs{}
				aep := AppendEntriesReply{}
				aea.Term = voteTerm
				aea.LeaderId = int64(rf.me)
				aea.LeaderCommit = rf.commitIndex
				aea.MatchIdx = rf.matchIndex[id]
				// aea.LogEntries = LogEntry{-1, -1, -1} // 心跳就是什么也不发送
				aea.LogEntries = make([]LogEntry, 0)
				aea.PrevLogIndex = -1
				aea.PrevLogTerm = -1
				rf.leaderLogMu.Unlock()

				Debug(dInfo, "S%v leader send heartbeat to %v term:%v [start_heartbeat]", rf.me, id, voteTerm)
				go rf.heartbeat(id, &aea, &aep, sendHeartBeatChan)
			}
			// 这个地方和request vote有一点不同，这里其实并不需要根据reply做太多的工作，所以这里可以直接go一个协程，回收chan就可以了
			// 也不需要timeout
			go func() {
				var set = 0
				for out := range sendHeartBeatChan {
					set++
					Debug(dInfo, "S%v leader recieve heartbeat from %v ok:%v [start_heartbeat]", rf.me, out.peer, out.ok)
					if out.ok {
						if rf.matchIndex[out.peer] < int64(len(rf.log)) {
							rf.consistent_checkMu.Lock()
							if rf.isStartConsistentCheck[int64(out.peer)] == false {
								go rf.consistent_check(int64(out.peer), voteTerm)
								rf.isStartConsistentCheck[int64(out.peer)] = true
							}
							rf.consistent_checkMu.Unlock()
						}
					}
					if out.r.Term > voteTerm {
						rf.serverState = follower
						break
					}
					if set == len(rf.peers)-1 {
						close(sendHeartBeatChan)
						break
					}
				}
			}()
			// wg.Done()
		} else {
			break
		}
		time.Sleep(heartBeatTime)
	}
}

func (rf *Raft) check_leader() {
	for rf.killed() == false {
		// rf == killed?
		if rf.serverState == leader {
			rf.leaderAlive = LeakBeat
			rf.findLeader = rf.me
			continue
		}
		Debug(dInfo, "S%v serverState:%v leaderAlive:%v findLeader:%v kill:%v [check_leader]", rf.me, rf.serverState, rf.leaderAlive, rf.findLeader, rf.killed())
		rf.leaderAlive = rf.leaderAlive - 1 // 这个地方不能直接bool型，得用一个2，1，0的变量，因为这里直接false了，上面就又重新开始选举leader了！term就变了
		if rf.leaderAlive <= 0 {
			rf.findLeader = -1
		}
		time.Sleep(checkLeaderTime)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.apply_msg = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.log = make([]LogEntry, 0)

	rf.serverState = follower
	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.findLeader = -1
	rf.leaderAlive = 0
	rf.majorityNum = len(rf.peers)/2 + 1
	rf.isStartConsistentCheck = make(map[int64]bool)
	for id := range rf.peers {
		rf.isStartConsistentCheck[int64(id)] = false
	}
	rf.IndexMu = make([]sync.Mutex, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.check_leader()
	go rf.commit_entries()

	return rf
}

func (rf *Raft) heartbeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, CC chan struct {
	ok   bool
	peer int64
	r    AppendEntriesReply
}) bool {
	// timeout := time.NewTimer(waitTime)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	// select {
	// case <-timeout.C:
	// 	timeout.Stop()
	// default:
	// 	timeout.Stop()
	// 	heartBeatChannel <- ok
	// }
	CC <- struct {
		ok   bool
		peer int64
		r    AppendEntriesReply
	}{ok, int64(server), *reply}
	return ok
}

type AppendEntriesArgs struct {
	// Your data here (3A, 3B).
	Term         int64
	LeaderId     int64
	PrevLogIndex int64
	PrevLogTerm  int64

	LogEntries   []LogEntry
	LeaderCommit int64

	// my add
	MatchIdx int64 //这个表示leader中，记录的与peer中的log相match的部分，只能apply这部分之前的内容，不然，有可能leadercommit很大，但是peer的内容并不match leader，导致做错东西
}

type AppendEntriesReply struct {
	// Your data here (3A).
	Term    int64
	Success bool

	// my add
	// AppliedIndex int64 //直接返回applied Index 作为match Index，不需要一个个往回找，一个个往回找，确定match Index太耗时间了，测试不通过
	XTerm  int64
	XIndex int64
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	term := args.Term
	leaderId := args.LeaderId
	PrevLogIndex := args.PrevLogIndex
	PrevLogTerm := args.PrevLogTerm

	LogEntries := args.LogEntries
	LeaderCommit := args.LeaderCommit
	MatchIdx := args.MatchIdx
	if MatchIdx == 0 {
		MatchIdx = rf.lastApplied
	}

	if len(LogEntries) == 0 {
		Debug(dInfo, "S%v term%v currentTerm:%v lastApplied:%v commitIndex:%v LeaderCommit:%v MatchIdx:%v Log.Index:%v Log.cmd:%v [AppendEntries]", rf.me, term, rf.CurrentTerm, rf.lastApplied, rf.commitIndex, LeaderCommit, MatchIdx, -1, -1)
	} else {
		Debug(dInfo, "S%v term%v currentTerm:%v lastApplied:%v commitIndex:%v LeaderCommit:%v MatchIdx:%v Log.Index:%v Log.cmd:%v prevLogIdx:%v prevLogTerm:%v len(log):%v[AppendEntries]", rf.me, term, rf.CurrentTerm, rf.lastApplied, rf.commitIndex, LeaderCommit, MatchIdx, LogEntries[0].Index, LogEntries[0].Log, PrevLogIndex, PrevLogTerm, len(LogEntries))
	}
	peer_log_idx := len(rf.log)

	if term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.Success = false
		reply.XIndex = -1
		reply.XTerm = -1
		// rf.commitMu.Unlock()
		return
	}
	// term >和=的逻辑基本是一样的
	// 无论是心跳还是增加，都需要的操作
	// term 相等的时候，heartbeat应该也是有效的，不然的话，leaderalive就没法刷新，会不断的产生新的term。
	if term > rf.CurrentTerm {
		rf.VotedFor = -1
		rf.persist()
	}
	rf.CurrentTerm = term
	rf.serverState = follower
	rf.findLeader = int(leaderId)
	rf.leaderAlive = LeakBeat
	reply.Term = term

	// rf.commitMu.Lock()
	// if LogEntries.Term == -1 && LogEntries.Index == -1 {
	if len(LogEntries) == 0 {
		// 心跳是空的，所以这里可以直接reply success
		reply.Success = true
		// reply.AppliedIndex = rf.lastApplied
	} else {
		// 心跳不空
		log_idx := LogEntries[0].Index
		if log_idx > int64(len(rf.log))+1 {
			// 太过超前
			reply.Success = false
			// reply.AppliedIndex = rf.lastApplied
			// rf.commitMu.Unlock()
			reply.XIndex = int64(len(rf.log))
			reply.XTerm = -1
			// rf.commitMu.Unlock()
			Debug(dClient, "in:%v", len(rf.log))
			return
		}

		if PrevLogIndex == -1 && PrevLogTerm == -1 {
			// check 的log是第一个log，一定成功
			// if len(rf.log) == 0 {
			// 	// rf.log = append(rf.log, LogEntries...)
			// 	rf.log = LogEntries
			// 	reply.Success = true
			// 	// } else {
			// 	// 	rf.log[0] = LogEntries
			// 	// 	reply.Success = true
			// }
			rf.log = LogEntries
			if len(rf.log) <= len(LogEntries) {
				rf.log = LogEntries
			}
			reply.Success = true
			// MatchIdx = 1
			MatchIdx = int64(len(LogEntries))
		} else {
			// PrevLogIndex很大，超出log范围的情况已经在log_idx > int64(peer_log_idx)+1去除了，所以prev_log一定存在
			prev_log := rf.log[PrevLogIndex-1]
			if PrevLogIndex == prev_log.Index && PrevLogTerm == prev_log.Term {
				// if log_idx == int64(len(rf.log))+1 {
				// 	// 用于append新的log
				// 	rf.log = append(rf.log[:PrevLogIndex], LogEntries...)
				// 	// } else {
				// 	// 	// 检查的是原来的
				// 	// 	rf.log[PrevLogIndex] = LogEntries
				// }
				// rf.log = append(rf.log[:PrevLogIndex], LogEntries...)

				// 当RPC乱序进入，必须保证更新后的log是最长的那个，不然的话，后续进来的短的log，将整个log的长度变短，导致leader上记录的log match很长，但实际很短
				// 不太能够直接这样判断，如果leader更换了，rf.log很长，但实际上的leader的log很短，那么这个leader的log就进不来了！要判断进来的log与原先的log是否相同！！！
				// if len(rf.log) <= len(LogEntries)+int(PrevLogIndex) {
				// 	for i := range LogEntries {
				// 		if i+int(PrevLogIndex) > len(rf.log)-1 {
				// 			rf.log = append(rf.log, LogEntries[i])
				// 		} else {
				// 			rf.log[i+int(PrevLogIndex)] = LogEntries[i]
				// 		}
				// 	}
				// }
				for i := range LogEntries {
					if i+int(PrevLogIndex) > len(rf.log)-1 {
						rf.log = append(rf.log, LogEntries[i])
					} else {
						rf.log[i+int(PrevLogIndex)] = LogEntries[i]
					}
				}
				reply.Success = true
				// MatchIdx = PrevLogIndex + 1
				MatchIdx = PrevLogIndex + int64(len(LogEntries))
			} else {
				reply.Success = false
				Debug(dClient, "or")
				reply.XTerm = prev_log.Term
				for id := range rf.log {
					if rf.log[id].Term == reply.XTerm {
						reply.XIndex = rf.log[id].Index
						// rf.commitMu.Unlock()
						return
					}
				}
			}
		}
	}
	// rf.commitMu.Unlock()

	// commit变化，需要执行新的command
	// 不能先执行，再加入新的command。顺序错了
	// 保证执行的log是leader已经执行了的，同时这部分log在follower和leader中存储的是一致的
	if LeaderCommit > rf.commitIndex && MatchIdx > rf.commitIndex {
		// 不能commit还不存在的log和还没确定match的log
		// rf.commitIndex为log长度，MatchIdx和LeaderCommit中的最小值
		if LeaderCommit <= int64(peer_log_idx) && LeaderCommit <= MatchIdx {
			rf.commitIndex = LeaderCommit
		} else if int64(peer_log_idx) <= LeaderCommit && int64(peer_log_idx) <= MatchIdx {
			rf.commitIndex = int64(peer_log_idx)
		} else if MatchIdx <= LeaderCommit && MatchIdx <= int64(peer_log_idx) {
			rf.commitIndex = MatchIdx
		}
		// 这个放在LeaderCommit>commitIndex里面应该是没问题的，因为只有commitIndex变化的时候，才有可能applied，不然前面的早已经applied了
		rf.persist()
	}
	// reply.AppliedIndex = rf.lastApplied
	// rf.commitMu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, CC chan struct {
	ok   bool
	peer int64
	r    AppendEntriesReply
}) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	CC <- struct {
		ok   bool
		peer int64
		r    AppendEntriesReply
	}{ok, int64(server), *reply}
	return ok
}

func (rf *Raft) consistent_check(peer int64, voteTerm int64) {
	for rf.killed() == false && rf.serverState == leader {
		rf.leaderLogMu.Lock()
		aea := AppendEntriesArgs{}
		aep := AppendEntriesReply{}
		aea.Term = voteTerm
		aea.LeaderId = int64(rf.me)
		aea.LeaderCommit = rf.commitIndex
		var index int64
		// 如果leader是刚刚当上的leader，这个时候，进入consistent_check，index就是超过rf.log了！
		// 直接就退出了，但是，实际上matchIndex为0，nextIndex是初始化的值，但是为了防止index=0。
		index = rf.nextIndex[peer]
		if rf.matchIndex[peer] == 0 && index != 1 {
			index = rf.nextIndex[peer] - 1
		}
		aea.MatchIdx = rf.matchIndex[peer]
		// 这里的逻辑应该是：leader没有复制的log，或者已经确定最新复制的log已经一致就不需要检查了
		if len(rf.log) == 0 || index > int64(len(rf.log)) {
			// 当leader没有log或者next index 已经超出了leader log的长度
			rf.leaderLogMu.Unlock()
			break
		} else {
			if index == 0 {
				Debug(dLeader, "S%v leader index error rf.matchIndex[%v]:%v rf.commitIndex:%v len(rf.log):%v [consistent_check]", rf.me, peer, rf.matchIndex[peer], rf.commitIndex, len(rf.log))
			}
			aea.LogEntries = make([]LogEntry, len(rf.log)-int(index)+1)
			copy(aea.LogEntries, rf.log[index-1:])
			if index == 1 {
				// index == 1表示，需要确认的log index=1，而index=1是不存在前一个log的，所以前一个log也是天然相同
				// -1表示不存在
				aea.PrevLogIndex = -1
				aea.PrevLogTerm = -1
			} else {
				aea.PrevLogIndex = rf.log[index-2].Index
				aea.PrevLogTerm = rf.log[index-2].Term
			}
		}
		rf.leaderLogMu.Unlock()
		Debug(dLeader, "S%v leader send follower:%v Log.Index:%v Log.cmd:%v len(log):%v[consistent_check]", rf.me, peer, aea.LogEntries[0].Index, aea.LogEntries[0].Log, len(aea.LogEntries))

		var sendEntry chan struct {
			ok   bool
			peer int64
			r    AppendEntriesReply
		} = make(chan struct {
			ok   bool
			peer int64
			r    AppendEntriesReply
		})
		timeout := time.NewTimer(waitTimeCheck)
		go rf.sendAppendEntries(int(peer), &aea, &aep, sendEntry)

		// 处理RPC
		select {
		case <-timeout.C:
			continue
		case out := <-sendEntry:
			if out.ok {
				aep = out.r
				if aep.Term > voteTerm {
					rf.serverState = follower
					break
				}
				Debug(dInfo, "S%v peer:%v aep.success:%v Log.Index:%v Log.cmd:%v len(log):%v", rf.me, peer, aep.Success, aea.LogEntries[0].Index, aea.LogEntries[0].Log, len(aea.LogEntries))
				if aep.Success {
					rf.IndexMu[out.peer].Lock()
					if rf.matchIndex[peer] < aea.LogEntries[len(aea.LogEntries)-1].Index {
						Debug(dClient, "len:%v", len(aea.LogEntries))
						rf.matchIndex[peer] = aea.LogEntries[len(aea.LogEntries)-1].Index
					}
					rf.nextIndex[peer] = rf.matchIndex[peer] + 1
					rf.IndexMu[out.peer].Unlock()
				} else {
					if aep.XTerm == -1 && aep.XIndex == -1 {
						break
					}
					// 这里的意思应该是，如果matchIndex是初始化过的，那么其实就可以直接给nextIndex了，不需要再去看Xterm和Xindex了！不然容易错
					if rf.matchIndex[peer] != 0 {
						rf.nextIndex[peer] = rf.matchIndex[peer] + 1
						continue
					}
					find_term := aep.XTerm
					var isfind bool = false
					rf.IndexMu[out.peer].Lock()
					for i := len(rf.log) - 1; i >= 0; i-- {
						if find_term == rf.log[i].Term {
							rf.nextIndex[peer] = int64(i + 1)
							isfind = true
							break
						}
					}
					if isfind == false && aep.XTerm == -1 {
						rf.nextIndex[peer] = aep.XIndex + 1
					} else if isfind == false && aep.XTerm != -1 {
						rf.nextIndex[peer] = aep.XIndex
					}
					rf.IndexMu[out.peer].Unlock()
				}
				Debug(dLeader, "S%v peer:%v matchIndex:%v nextIndex:%v [consistent_check]", rf.me, peer, rf.matchIndex[peer], rf.nextIndex[peer])
				go func() {
					var sort_array []int64
					sort_array = make([]int64, 0)
					for id := range rf.matchIndex {
						if rf.me == id {
							continue
						}
						sort_array = append(sort_array, rf.matchIndex[id])
					}
					sort.SliceStable(sort_array, func(i, j int) bool {
						return sort_array[i] < sort_array[j]
					})
					min_match_idx := sort_array[rf.majorityNum-1]
					Debug(dLeader, "S%v leader min_match_idx:%v [consistent_check]", rf.me, min_match_idx)
					rf.leaderLogMu.Lock()
					if min_match_idx > rf.commitIndex && rf.log[min_match_idx-1].Term == rf.CurrentTerm {
						// commitIndex 变大之后，要执行command才行
						rf.commitIndex = min_match_idx
						// 这里是否存在可能，当commit结束后，persist还没开始前，就已经crash了，那么persist记录的commit位置就不准确了
						rf.persist()
					}
					rf.leaderLogMu.Unlock()
				}()
			}
		}
		// time.Sleep(consistent_checkTime)
	}

	rf.consistent_checkMu.Lock()
	rf.isStartConsistentCheck[int64(peer)] = false
	rf.consistent_checkMu.Unlock()
}

func (rf *Raft) commit_entries() {
	for !rf.killed() {
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			apply_msg := ApplyMsg{}
			apply_msg.CommandValid = true
			apply_msg.Command = rf.log[i-1].Log
			apply_msg.CommandIndex = int(rf.log[i-1].Index)
			rf.apply_msg <- apply_msg
			Debug(dInfo, "S%v execute log entry idx:%v command%v commitIndex:%v [commit_entries]", rf.me, int(rf.log[i-1].Index), apply_msg.Command, rf.commitIndex)
		}
		rf.lastApplied = rf.commitIndex
		rf.persist()
		time.Sleep(waitTimeCommit)
	}
}
