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
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	Follower  = iota
	Candidate = iota
	Leader    = iota
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

type LogEntry struct {
	Command interface{}
	Term    int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor    int
	log         []LogEntry

	lastLogIndex int // 最后一条日志条目的索引
	leaderId     int // 领导人的 Id

	commitIndex       int
	lastApplied       int
	lastIncludedIndex int // 快照中包含的最后日志条目的索引值
	lastIncludedTerm  int // 快照中包含的最后日志条目的任期号

	nextIndex  []int
	matchIndex []int

	state          int
	electionTimer  *time.Timer
	heartbeatTimer *time.Timer
	applyCh        chan ApplyMsg
	applyCond      *sync.Cond

	dead int32 // set by Kill()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (3A).
	return rf.currentTerm, rf.state == Leader
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.SaveRaftState(raftstate)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var CurrentTerm int
	var VotedFor int
	var Log []LogEntry
	var LastIncludedIndex int
	var LastIncludedTerm int
	if d.Decode(&CurrentTerm) != nil || d.Decode(&VotedFor) != nil || d.Decode(&Log) != nil ||
		d.Decode(&LastIncludedIndex) != nil || d.Decode(&LastIncludedTerm) != nil {
		//error
	} else {
		rf.currentTerm = CurrentTerm
		rf.votedFor = VotedFor
		rf.log = Log
		rf.lastIncludedIndex = LastIncludedIndex
		rf.lastIncludedTerm = LastIncludedTerm
		rf.commitIndex = rf.lastIncludedIndex
		rf.lastApplied = rf.lastIncludedIndex
		rf.lastLogIndex = rf.lastIncludedIndex + len(rf.log) - 1

		DPrintf("raft %v readPersist, CurrentTerm %v, VotedFor %v, Log %v, LastLogIndex %v", rf.me, rf.currentTerm, rf.votedFor, rf.log, rf.lastLogIndex)
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

type InstallSnapshotArgs struct {
	Term              int    //领导者的任期
	LeaderId          int    //领导者的 Id，以便于跟随者重定向请求
	LastIncludedIndex int    //快照中包含的最后日志条目的索引值
	LastIncludedTerm  int    //快照中包含的最后日志条目的任期号
	Data              []byte //快照数据
}

type InstallSnapshotReply struct {
	Term int //当前任期号，用于领导者去更新自己
}

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// If candidate's term is less than current term
	if args.Term < rf.currentTerm {
		return
	}

	// If candidate's term is greater than current term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
	}

	// Check if we can vote for this candidate
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		(args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term &&
				args.LastLogIndex >= len(rf.log)-1)) {

		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimer()
	}

	rf.persist()

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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) becomeCandidate() {

	rf.state = Candidate
	rf.currentTerm++
	rf.persist()

	DPrintf("raft %v become candidate, term %v", rf.me, rf.currentTerm)
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// Your code here (3A)
		// Check if a leader election should be started.
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.mu.Unlock()
				continue
			}
			rf.becomeCandidate()
			rf.startElection()
			rf.resetElectionTimer()
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.broadcast()
				rf.resetHeartbeatTimer()
			}
			rf.mu.Unlock()
		}

	}
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(randomElectionTimeout())
}

func (rf *Raft) resetHeartbeatTimer() {
	rf.heartbeatTimer.Stop()
	rf.heartbeatTimer.Reset(fixedHeartbeatTimeout())
}

func (rf *Raft) startElection() {
	if rf.state == Candidate {
		//给自己投票
		rf.votedFor = rf.me
		rf.persist()
		//选票数
		voteCount := 1

		//给所有其他节点发送投票请求
		for peer := range rf.peers {
			if peer != rf.me {
				//异步调用自检
				if rf.state != Candidate {
					return
				}
				args := RequestVoteArgs{}
				args.Term = rf.currentTerm
				args.CandidateId = rf.me
				args.LastLogIndex = rf.lastLogIndex
				args.LastLogTerm = rf.log[args.LastLogIndex-rf.lastIncludedIndex].Term

				reply := RequestVoteReply{}
				DPrintf("raft %v send RequestVote to %v, args.Term %v, args.LastLogIndex %v, args.LastLogTerm %v", rf.me, peer, args.Term, args.LastLogIndex, args.LastLogTerm)
				go rf.requestVote(peer, &args, &reply, &voteCount)
			}
		}
	}
}

func (rf *Raft) requestVote(peer int, args *RequestVoteArgs, reply *RequestVoteReply, voteCount *int) {

	ok := rf.sendRequestVote(peer, args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		DPrintf("raft %v receive RequestVote reply from %v, reply.Term %v, reply.VoteGranted %v", rf.me, peer, reply.Term, reply.VoteGranted)

		//如果收到旧的 RPC 的回复，先记录回复中的任期（可能高于您当前的任期），
		//然后将当前任期与您在原始 RPC 中发送的任期进行比较。如果两者不同，请放弃回复并返回
		if rf.currentTerm != args.Term || rf.state != Candidate {
			return
		}

		if reply.VoteGranted {
			*voteCount++
			//如果获得了大多数的选票，就成为领导人
			if *voteCount > len(rf.peers)/2 {
				rf.becomeLeader()
				rf.broadcast()
				rf.resetHeartbeatTimer()
			}
		} else if reply.Term > rf.currentTerm {
			//如果对方的任期号比自己大，就转换为跟随者
			rf.state = Follower
			rf.votedFor = -1
			rf.currentTerm = reply.Term
			rf.resetElectionTimer()
			rf.persist()
		}
	}
}

func (rf *Raft) sendSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
}

func (rf *Raft) broadcast() {

	if rf.state != Leader {
		return
	}

	for peer := range rf.peers {
		if peer != rf.me {
			//检查是否发送快照
			if rf.nextIndex[peer] <= rf.lastIncludedIndex {
				args := InstallSnapshotArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.LastIncludedIndex = rf.lastIncludedIndex
				args.LastIncludedTerm = rf.lastIncludedTerm
				args.Data = rf.persister.ReadSnapshot()

				reply := InstallSnapshotReply{}

				DPrintf("raft %v send InstallSnapshot to %v, args.Term %v, args.LastIncludedIndex %v, args.LastIncludedTerm %v, args.Data %v", rf.me, peer, args.Term, args.LastIncludedIndex, args.LastIncludedTerm, args.Data)

				go rf.sendSnapshot(peer, &args, &reply)
			} else {
				//发送日志
				args := AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderId = rf.me
				args.PrevLogIndex = rf.nextIndex[peer] - 1
				args.PrevLogTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term
				args.LeaderCommit = rf.commitIndex
				//表示心跳， 为空
				args.Entries = make([]LogEntry, 0)
				//如果存在日志条目要发送，就发送日志条目
				if rf.nextIndex[peer] <= rf.lastLogIndex {
					//深拷贝
					args.Entries = append(args.Entries, rf.log[rf.nextIndex[peer]-rf.lastIncludedIndex:]...)
				}

				reply := AppendEntriesReply{}
				DPrintf("raft %v send AppendEntries to %v, args.Term %v, args.PrevLogIndex %v, args.PrevLogTerm %v, args.LeaderCommit %v, args.Entries %v", rf.me, peer, args.Term, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries)
				go rf.sendLog(peer, &args, &reply)
			}
		}
	}
}

// 发送日志(空时，表示心跳)
func (rf *Raft) sendLog(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply) {

	ok := rf.sendAppendEntries(peer, args, reply)

	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		DPrintf("raft %v receive AppendEntries reply from %v, reply.Term %v, reply.Success %v, reply.ConflictIndex %v, reply.ConflictTerm %v", rf.me, peer, reply.Term, reply.Success, reply.ConflictIndex, reply.ConflictTerm)

		//如果收到旧的 RPC 的回复，先记录回复中的任期（可能高于您当前的任期），
		//然后将当前任期与您在原始 RPC 中发送的任期进行比较。如果两者不同，请放弃回复并返回
		if rf.currentTerm != args.Term || rf.state != Leader {
			return
		}

		//处理返回体
		//如果成功，更新 nextIndex 和 matchIndex
		if reply.Success == true {
			rf.nextIndex[peer] = Max(args.PrevLogIndex+len(args.Entries)+1, rf.nextIndex[peer])
			rf.matchIndex[peer] = Max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[peer])

			//如果存在一个满足 N > commitIndex 的 N，并且大多数的 matchIndex[i] ≥ N 成立，并且 log[N].term == currentTerm 成立，那么令 commitIndex 等于这个 N
			for N := rf.commitIndex + 1; N <= rf.lastLogIndex; N++ {
				if rf.log[N-rf.lastIncludedIndex].Term == rf.currentTerm {
					count := 1
					for i := range rf.peers {
						if i != rf.me && rf.matchIndex[i] >= N {
							count++
						}
					}
					if count > len(rf.peers)/2 {
						rf.commitIndex = N
						//break
					}
				}
			}
			rf.applyCond.Broadcast()
		} else {
			//如果对方的任期号比自己大，就转换为跟随者
			if reply.Term > rf.currentTerm {
				rf.state = Follower
				rf.votedFor = -1
				rf.currentTerm = reply.Term
				rf.resetElectionTimer()
				rf.persist()
			} else {
				//如果失败，就递减 nextIndex 重试
				// rf.NextIndex[peer]--
				// if rf.NextIndex[peer] < 1 {
				// 	rf.NextIndex[peer] = 1
				// }

				if reply.ConflictTerm == -1 {
					rf.nextIndex[peer] = reply.ConflictIndex
				} else {
					conflictIndex := -1
					//在收到一个冲突响应后，领导者首先应该搜索其日志中任期为 conflictTerm 的条目。
					//如果领导者在其日志中找到此任期的一个条目，则应该设置 nextIndex 为其日志中此任期的最后一个条目的索引的下一个。
					for i := args.PrevLogIndex; i >= rf.lastIncludedIndex; i-- {
						if rf.log[i-rf.lastIncludedIndex].Term == reply.ConflictTerm {
							conflictIndex = i
							break
						}
					}
					//如果领导者没有找到此任期的条目，则应该设置 nextIndex = conflictIndex
					if conflictIndex == -1 {
						rf.nextIndex[peer] = reply.ConflictIndex
					} else {
						rf.nextIndex[peer] = conflictIndex + 1
					}
				}
			}
		}
	}
}

func (rf *Raft) becomeLeader() {
	if rf.state != Candidate {
		return
	}

	//转换为领导者
	rf.state = Leader
	rf.leaderId = rf.me
	//初始化 nextIndex 和 matchIndex
	//初始化所有的 nextIndex 值为自己的最后一条日志的 index 加 1
	for i := range rf.peers {
		//rf.NextIndex[i] = rf.LastLogIndex + 1
		rf.nextIndex[i] = Max(rf.lastLogIndex+1, rf.nextIndex[i])
		rf.matchIndex[i] = 0
	}

	DPrintf("raft %v become leader, term %v", rf.me, rf.currentTerm)
}

// AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

// AppendEntries RPC handler.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//DPrintf("raft %v receive AppendEntries, args.PrevLogIndex %v, args.PrevLogTerm %v, rf.LastLogIndex %v, rf.LastIncludedIndex %v", rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.LastLogIndex, rf.LastIncludedIndex)

	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	//如果args.Term  < rf.CurrentTerm, 返回 false
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	//如果 args.Term  > rf.CurrentTerm，切换状态为跟随者
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.persist()
	}

	rf.state = Follower
	rf.leaderId = args.LeaderId
	rf.resetElectionTimer()

	//如果接收者日志中没有包含这样一个条目，那么就返回 false
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		reply.ConflictTerm = -1
		return
	}

	//在接收者日志中, 如果找不到PrevLogIndex
	//应该返回 conflictIndex = len(log) 和 conflictTerm = None。
	if args.PrevLogIndex > rf.lastLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictIndex = rf.lastLogIndex + 1
		reply.ConflictTerm = -1
		return
	}

	//在接收者日志中, 如果追随者的日志中有 preLogIndex，但是任期不匹配
	//返回 conflictTerm = log[preLogIndex].Term，然后在它的日志中搜索任期等于 conflictTerm 的第一个条目索引。
	if args.PrevLogIndex >= rf.lastIncludedIndex && rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictTerm = rf.log[args.PrevLogIndex-rf.lastIncludedIndex].Term
		index := args.PrevLogIndex
		for index >= rf.lastIncludedIndex && rf.log[index-rf.lastIncludedIndex].Term == reply.ConflictTerm {
			index--
		}
		reply.ConflictIndex = index + 1
		return
	}

	DPrintf("raft %v start AppendEntries, args.PrevLogIndex %v, args.PrevLogTerm %v, rf.LastLogIndex %v, rf.LastIncludedIndex %v", rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.lastLogIndex, rf.lastIncludedIndex)
	//如果一个已经存在的条目和新条目（即刚刚接收到的日志条目）发生了冲突（因为索引相同，任期不同），那么就删除这个已经存在的条目以及它之后的所有条目
	if len(args.Entries) > 0 {
		for i, entry := range args.Entries {
			if args.PrevLogIndex+i+1 > rf.lastLogIndex {
				rf.log = append(rf.log, entry)
				rf.lastLogIndex++
			} else {
				if rf.log[args.PrevLogIndex+i+1-rf.lastIncludedIndex].Term != entry.Term {
					rf.log = rf.log[:args.PrevLogIndex+i+1-rf.lastIncludedIndex]
					rf.log = append(rf.log, entry)
					rf.lastLogIndex = args.PrevLogIndex + i + 1
				}
			}
		}
	}

	rf.persist()

	//如果leaderCommit > commitIndex
	//则把commitIndex 重置为  leaderCommit 或者是 上一个新条目的索引 取两者的最小值
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if rf.commitIndex > rf.lastLogIndex {
			rf.commitIndex = rf.lastLogIndex
		}
		rf.applyCond.Broadcast()
	}

	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		term := rf.currentTerm
		rf.mu.Unlock()

		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			go func(peer int) {
				args := AppendEntriesArgs{}
				args.Term = term
				args.LeaderId = rf.me

				reply := AppendEntriesReply{}
				rf.sendAppendEntries(peer, &args, &reply)
			}(peer)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func randomElectionTimeout() time.Duration {
	return time.Duration(300+rand.Intn(200)) * time.Millisecond
}

func fixedHeartbeatTimeout() time.Duration {
	return time.Millisecond * 100
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
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.applyCh = applyCh

	// Your initialization code here (3A, 3B, 3C).
	rf.state = Follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.log[0] = LogEntry{Term: 0}

	rf.lastLogIndex = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	for i := range rf.peers {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	rf.electionTimer = time.NewTimer(randomElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(randomElectionTimeout())

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
