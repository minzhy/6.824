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

	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int32

const (
	follower State = iota
	candidate
	leader
)

var waitTime time.Duration = 2 * time.Millisecond
var sendRequestVoteChannel = make(chan bool)
var heartBeatChannel = make(chan bool)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	voteForMu sync.Mutex
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state // 断电之后，一定要保存的（一定不能缺失的东西
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 去看test，这个Raft相当于每个peer的结构，test的时候，会创建n个peer，也就是这个结构体，每个peer里面，保存了别人的地址peers
	// Temporary persistent state
	CurrentTerm int64
	VotedFor    int64
	log         []int64

	// Volatile state on all servers
	commitIndex int64
	lastApplied int64

	// Volatile state on leaders
	serverState State
	nextIndex   []int64
	matchIndex  []int64

	// my add
	collectVotes       int64
	findLeader         int
	leaderAlive        int
	majorityNum        int
	currentTermIsVoted bool
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
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
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

	// debug
	VoteFollowerId int64
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	term := args.Term
	candidateId := args.CandidateId
	// lastLogIndex := args.LastLogIndex
	// lastLogTerm := args.LastLogTerm

	if term < rf.CurrentTerm {
		reply.Term = rf.CurrentTerm
		reply.VoteGranted = false
		return
	} else if term > rf.CurrentTerm {
		rf.CurrentTerm = term
		rf.serverState = follower

		// 这里应该是不用判断rf.serverState == leader || rf.findLeader != -1的，因为都是follower状态了，不需要所有follower都意识到没有leader才投票
		// term 增加了重新获得投票资格
		rf.voteForMu.Lock()
		rf.VotedFor = candidateId
		rf.currentTermIsVoted = true
		reply.Term = rf.CurrentTerm
		reply.VoteFollowerId = int64(rf.me)
		reply.VoteGranted = true
		rf.voteForMu.Unlock()
		return
	} else if term == rf.CurrentTerm {
		// 这里需要补充判断log index
		rf.voteForMu.Lock()
		if !rf.currentTermIsVoted && rf.VotedFor == -1 {
			rf.VotedFor = candidateId
			rf.currentTermIsVoted = true
			reply.Term = rf.CurrentTerm
			reply.VoteFollowerId = int64(rf.me)
			reply.VoteGranted = true
		} else {
			reply.Term = rf.CurrentTerm
			reply.VoteGranted = false
		}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	timeout := time.NewTimer(waitTime)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	select {
	case <-timeout.C:
		timeout.Stop()
	default:
		timeout.Stop()
		sendRequestVoteChannel <- ok
	}
	return ok
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
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
			rf.currentTermIsVoted = true
			// atomic.AddInt64(&rf.collectVotes, 1)
			rf.collectVotes += 1
			// fmt.Println("me", rf.me, "vote", rf.me, "in term", rf.CurrentTerm)
			rf.voteForMu.Unlock()

			// sendRequestVote
			for id := range rf.peers {
				if id == rf.me {
					continue
				}
				rva := RequestVoteArgs{}
				rvr := RequestVoteReply{}
				rva.Term = voteTerm
				rva.CandidateId = int64(rf.me)

				// before := time.Now()
				// //这个地方小心，因为如果是disconnect或者是kill的话，联系不上对面，这个函数返回ok的时间会特别长，一般的请求在400μs内，但是出现错误的请求在600左右的ms差了1000倍左右！
				// ok := rf.sendRequestVote(id, &rva, &rvr)
				// // after := time.Now()
				// fmt.Println(time.Since(before), "me", rf.me, "term", rf.CurrentTerm)

				timeout := time.NewTimer(waitTime) //timer和ticker区别在于，ticker是循环，但是timer是只记录一次时间，而不是每次时间过后，需要reset重新设置时间

				go rf.sendRequestVote(id, &rva, &rvr)

				var ok bool
				select {
				case <-timeout.C:
					timeout.Stop()
					continue //这里continue是全跳了for后面的语句吗？
				case ok = <-sendRequestVoteChannel:
					timeout.Stop()
				}

				if !ok {
					continue
				}
				term := rvr.Term
				if term <= voteTerm && rvr.VoteGranted {
					rf.collectVotes += 1
					// fmt.Println("me", rvr.VoteFollowerId, "vote", rf.me, "in term", rf.CurrentTerm)
				} else if term > voteTerm {
					rf.collectVotes = 0
					rf.serverState = follower
					goto EndVote
				}
			}
			// win election
			if rf.collectVotes >= int64(rf.majorityNum) {
				rf.serverState = leader
				rf.leaderAlive = 2
				rf.findLeader = rf.me
				// heartbeat
				// 当选的是voteTerm的leader
				go func(voteTerm int64) {
					for {
						rf.mu.Lock()
						if rf.serverState == leader && !rf.killed() && voteTerm == rf.CurrentTerm {
							// var wg sync.WaitGroup
							for id := range rf.peers {
								if id == rf.me {
									continue
								}
								aea := AppendEntriesArgs{}
								aep := AppendEntriesReply{}
								aea.Term = voteTerm
								aea.LeaderId = int64(rf.me)
								timeout := time.NewTimer(waitTime) //timer和ticker区别在于，ticker是循环，但是timer是只记录一次时间，而不是每次时间过后，需要reset重新设置时间

								go rf.heartbeat(id, &aea, &aep)

								var ok bool
								select {
								case <-timeout.C:
									timeout.Stop()
									continue //这里continue是全跳了for后面的语句吗？
								case ok = <-heartBeatChannel:
									timeout.Stop()
								}

								if ok {
									if aep.Term > voteTerm {
										rf.serverState = follower
									}
								}

							}
							// wg.Done()
						} else {
							rf.mu.Unlock()
							break
						}
						rf.mu.Unlock()
						time.Sleep(time.Duration(150) * time.Millisecond)
					}
				}(voteTerm)
			} else {
				rf.serverState = follower
			}
		EndVote:
			// fmt.Println("[ticker]::vote collection vote", rf.collectVotes, "me", rf.me, "term", voteTerm)
			// clear state
			rf.collectVotes = 0
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) check_leader() {
	for {
		// rf == killed?
		if rf.serverState == leader {
			rf.leaderAlive = 2
			rf.findLeader = rf.me
			continue
		}
		rf.leaderAlive = rf.leaderAlive - 1 // 这个地方不能直接bool型，得用一个2，1，0的变量，因为这里直接false了，上面就又重新开始选举leader了！term就变了
		if rf.leaderAlive <= 0 {
			rf.findLeader = -1
		}
		time.Sleep(time.Duration(200) * time.Millisecond)
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

	// Your initialization code here (3A, 3B, 3C).
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.log = make([]int64, 0)

	rf.serverState = follower
	rf.nextIndex = make([]int64, 0)
	rf.matchIndex = make([]int64, 0)

	rf.collectVotes = 0
	rf.findLeader = -1
	rf.leaderAlive = 0
	rf.majorityNum = len(rf.peers)/2 + 1
	rf.currentTermIsVoted = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.check_leader()

	return rf
}

func (rf *Raft) heartbeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	timeout := time.NewTimer(waitTime)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	select {
	case <-timeout.C:
		timeout.Stop()
	default:
		timeout.Stop()
		heartBeatChannel <- ok
	}
	return ok
}

type AppendEntriesArgs struct {
	// Your data here (3A, 3B).
	Term         int64
	LeaderId     int64
	PrevLogIndex int64
	PrevLogTerm  int64

	Entries      []int64
	LeaderCommit int64
}

type AppendEntriesReply struct {
	// Your data here (3A).
	Term    int64
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (3A, 3B).
	term := args.Term
	leaderId := args.LeaderId
	// PrevLogIndex := args.PrevLogIndex
	// PrevLogTerm := args.PrevLogTerm

	// Entries := args.Entries
	// LeaderCommit := args.LeaderCommit
	// fmt.Println(term, rf.CurrentTerm)
	if term > rf.CurrentTerm {
		rf.CurrentTerm = term
		rf.serverState = follower
		rf.findLeader = int(leaderId)
		rf.leaderAlive = 2
		reply.Term = term
	} else if term < rf.CurrentTerm {
		// fmt.Println("[AppendEntries]::term < rf.currentTerm")
		reply.Term = rf.CurrentTerm
	} else if term == rf.CurrentTerm {
		// term 相等的时候，heartbeat应该也是有效的，不然的话，leaderalive就没法刷新，会不断的产生新的term。
		rf.serverState = follower
		rf.findLeader = int(leaderId)
		rf.leaderAlive = 2
		reply.Term = term
	}
}
