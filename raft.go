package raft

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	pb "github.com/coreos/etcd/raft/raftpb"
)

//当没有leader时占位用的Node id
const None uint64 = 0
const noLimit = math.MaxUint64

//集群中node的身份
type StateType uint64
const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
	StatePreCandidate
	numStates
)
var stmap = [...] string {
	"StateFollower",
	"StateCandidate",
	"StateLeader",
	"StatePreCandidate",
}
func (st StateType) String() string {
	return stmap[uint64(st)]
}
type ReadOnlyOption int

//TODO
//具体的含义还需要继续分析
const (
	//保证大多数只读线性特性
	ReadOnlySafe	ReadOnlyOption = iota
	//在leader下保证只读的线性特性
	ReadOnlyLeaseBased
)

type CampaignType string
const (
	//当Config.PreVote为true时，正常选举的第一个阶段
	campaignPreElection	CampaignType = "campaignPreElection"
	//第二阶段
	campaignElection	CampaignType = "campaignElection"
	//leader transfer
	campaignTransfer	CampaignType = "campaignTransfer"
)

type lockedRand struct {
	mu sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	v:= r.rand.Intn(n)
	return v
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

type Config struct {
	ID uint64
	
	peers []uint64

	learners []uint64

	ElectionTick int

	HeartbeatTick int

	Storage Storage

	Applied uint64

	//限制每一条append msg的最大size
	//越小恢复的越快
	MaxSizePerMsg	uint64

	//heart中携带数据log的最大值
	MaxInflightMsgs int

	CheckQuorum bool

	PreVote bool

	ReadOnlyOption ReadOnlyOption

	Logger Logger

	DisableProposalForwarding bool
}

func(c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}
	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}
	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}
	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}
	if c.MaxInflightMsgs <= 0 {
		return errors.New("max inflight msg must be greater than 0")
	}

	if c.Logger == nil {
		c.Logger = raftLogger
	}
	if c.ReadOnlyOption == ReadOnlyLeaseBased && !c.CheckQuorum {
		return errors.New("CheckQuorum must be enable when ReadOnlyOption is ReadOnlyLeaseBased")
	}
	return nil
}

type raft struct {
	id uint64

	Term uint64
	Vote uint64

	readStates []ReadState

	raftLog *raftLog

	maxInflight int
	maxMsgSize  uint64
	prs	map[uint64]*Progress
	learnerPrs map[uint64]*Progress

	state StateType

	isLearner bool

	votes map[uint64]bool

	//TODO 
	//消息缓存,发给谁的消息???发送给多个对端的消息，如何判断真正是发给那一个对端的
	msgs []pb.Message

	//leader id
	lead uint64

	//leader变更的目标
	leadTransferee uint64

	//若存在未应用的配置，新配置将被忽略
	pendingConf bool

	readOnly *readOnly

	electionElapsed int

	heartbeatElapsed int

	checkQuorum bool
	preVote  bool
	
	heartbeatTimeout int
	electionTimeout int

	randomizedElectionTimeout int
	disableProposalForwarding bool

	tick func()
	step stepFunc
	
	logger Logger
}

func newRaft(c *Config) *raft {
	if err := c.validate;err != nil {
		panic(err.Error())
	}
	raftlog := newLog(c.Storage,c.Logger)
	hs,cs,err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}

	//从配置和confstate中读取集群信息
	peers := c.peers
	learners := c.learners

	if len(cs.Nodes) > 0 || len(cs.Learners) > 0  {
		if len(peers) > 0 || len(learners) > 0 {
			//原先保存的配置中有集群中的节点，创建新的节点的时候又指定了新的集群配置，
			//这种情况下默认是不识别直接退出
			panic("cannot specify both newRaft(peers,learners) and confstate(Nodes,Learners)")
		}
		peers = cs.Nodes
		learners = cs.Learners
	}

	r := &raft {
		id:					c.ID,
		lead:				None,
		isLearner:			false,
		raftLog:			raftLog,
		maxMsgSize:			c.MaxSizePerMsg,
		maxInflight:		c.MaxInflightMsgs,
		prs:				make(map[uint64]*Progress),
		learnersPrs:		make(map[uint64]*Progress),
		electionTimeout:	c.ElectionTick,
		heartbeatTimeout:	c.HeartbeatTick,
		logger:				c.Logger,
		checkQuorum:		c.CheckQuorum,
		preVote:			c.PreVote,
		readOnly:			newReadOnly(c.ReadOnlyOption),
		disableProposalForwarding: c.DisableProposalForwarding,
	} 

	//将集群中的信息扔到Progress中
	for _,p :=range peers {
		r.prs[p] = &Progress{Next:1,ins:newInflight(r.maxInflight)}
	}
	for _,p :=range learners {
		if _,ok := r.prs[p];ok {
			panic(fmt.Sprintf("node %x is in both learn and peer list",p))
		}
		r.learnerPrs[p] = &Progress{Next:1,ins:newInflight(r.maxInflight),IsLearner:true}
		if r.id == p {
			r.isLearner = true
		}
	}

	//载入hardState
	if ! isHardStateEqual(hs,emptyState) {
		r.loadState(hs)
	}
	//读取日志中的相关信息
	if c.Applied >0 {
		raftLog.appliedTo(c.Applied)
	}
	
	r.becomeFollower(r.Term,None)
	var nodesStrs []string
	for _,n := range r.nodes() {
		nodesStrs = append(nodesStrs,fmt.Sprintf("%x",n))
	}

	r.logger.Infof("newRaft %x [peers:[%s],term: %d,commit: %d,applied: %d,lastindex: %d,lastterm: %d ]",
		r.id,strings.Join(nodesStrs,","),r.Term,r.raftLog.committed,r.raftLog.applied,r.raftLog.lastIndex(),r.raftLog.lastTerm())

	return r
}

func (r *raft) hasLeader() bool {	return r.lead != None	}

func(r *raft) softState() * SoftState {
	return &SoftState{Lead:r.lead,RaftState:r.state}
}

func (r *raft) hardState() pb.HardState {
	return pb.HardState {
		Term: r.Term,
		Vote: r.Vote,
		Commit:	r.raftLog.committed,
	}
}

func (r *raft) quorum() int {
	return len(r.prs)/2+1
}

func (r *raft) nodes() []uint64 {
	nodes := make([]uint64,0,len(r.prs)+len(r.learnerPrs))

	for id := range  r.prs {
		nodes = append(nodes,id)
	}
	for id := range r.learnerPrs {
		nodes = append(nodes,id)
	}

	sort.Sort(uint64Slice(nodes))
	return nodes
}

//保存到storage中然后发送到mailbox中
func (r *raft) send(m pb.Message) {
	m.From = r.id 
	if m.Type == pb.MsgVote || m.Type == pb.MsgVoteResp || m.Type == pb.MsgPreVote || m.Type == pb.MsgPreVoteResp {
		if m.Term == 0 {
			//所有竞选信息需要在发送信息的时候设置term
			//MsgVote:参加竞选的term编号，参加竞选时增加肯定不为0
			//MsgVoteResp:是reply的term。若被选上，肯定也不为0
			//MsgPreVote:将要参加竞选的term，因为要根据当前term推测下一个term用来竞选，所以不为0
			//MsgPreVoteResp:若prevote被接受，none zero
			panic(fmt.Sprintf("term should be set when send %s",m.Type))
		}
	}else {
		//当发送正常信息的时候term不应该被设置,
		if m.Term != 0 {
			panic(fmt.Sprintf("term should be set when sending %s (was %d)",m.Type,m.Term))
		}
		//被设置为当前raft的term编号
		if m.Type != pb.MsgProp && m.Type != pb.MsgReadIndex {
			m.Term = r.Term
		}
	}
	r.msgs = append(r.msgs,m)
}

func (r *raft) sendHeartbeat(to uint64,ctx []byte) {
	、、m:=
}

func (r *raft) getProgress(id uint64 ) *Progress {
	if pr,ok := r.prs[id]; ok {
		return pr
	}
	return r.learnerPrs[id]
}

// 通过rpc将entries发送到指定的对端
func (r *raft) sendAppend(to uint64) {
	pr := r.getProgress(to)

	if pr.IsPaused() {
		return 
	}

	m := pb.Message{}
	m.To = to
	term,errt := r.raftLog.term(pr.Next -1)
	ents,erre := r.raftLog.entries(pr.Next,r.maxMsgSize)

	//若获取term和entries失败，发送snapshot给对端,并将对端progress设置为snapshot
	if errt != nil || erre != nil {
		if !pr.RecentActive {
			//对端可能挂了
			r.logger.Debugf("ignore sending snapshot to %x since it is not recently active ",to )
			return 
		}
		m.Type = pb.MsgSnap
		snapshot,err := r.raftLog.snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				r.logger.Debugf("%x failed to send snapshot to %x because snapshot is temporarily unavailable",r.id,to)
				return 
			}
			panic(err)
		}
		if IsEmptySnap(snapshot) {
			panic("need non-empty snapshot")
		}
		m.Snapshot = snapshot
		sindex,sterm := snapshot.Metadata.Index,snapshot.Metadata.Term
		r.logger.Debugf("%x [firstindex: %d, commit: %d] send snapshot[index: %d,term: %d] to %x [%s]",
			r.id,r.raftLog.firstIndex(),r.raftLog.committed,sindex,sterm,to,pr)
		pr.becomeSnapshot(sindex)
		r.logger.Debugf("%x paused sending replication messages to %s [%s]",r.id,to,pr)
	}else {
		//正常发送消息。根据对端的progress状态决定.
		//发送消息到raft的msg中，在progress中保存此消息的inflights
		m.Type = pb.MsgApp
		m.Index = pr.Next -1
		m.LogTerm = term
		m.Entries = ents
		m.Commit = r.raftLog.committed
		if n:= len(m.Entries); n != 0 {
			switch pr.State {
			case ProgressStateReplicate:
				last := m.Entries[n-1].Index
				pr.optimisticUpdate(last)
				pr.ins.add(last)
			case ProgressStateProbe:
				pr.pause()
			default:
				r.logger.Panicf("%x is sending append in unhandles state %s",r.id,pr.State)
			}
		}
	}
	r.send(m)
}

type stepFunc func(r *raft, m pb.Message)

// 领导者对消息的处理过程
func stepLeader(r *raft,m pb.Message) {
	switch m.Type {
	case pb.MsgBeat:
	case pb.MsgCheckQuorum:
	case pb.MsgProp:
	case pb.MsgReadIndex:
	}

	//所有其他的消息类型需要一个progress来进行
	pr := r.getProgress(m.From)
	if pr == nil {
		r.logger.Debugf("%x no progress available for %x",r.id,m.From)
	}

	switch m.Type {
	case pb.MsgAppResp:
	case pb.MsgHeartbeatResp:
	case pb.MsgSnapStatus:
	case pb.MsgUnreachable:
	case pb.MsgTransferLeader:
	}
}

func stepCandidate(r *raft,m pb.Message) {
	var myVoteRespType pb.MessageType

	if r.state == StatePreCandidate {
		myVoteRespType = pb.MsgPreVoteResp
	}else {
		myVoteRespType = pb.MsgVoteResp
	}

	switch m.Type {
	case pb.MsgProp:
	case pb.MsgApp:
	case pb.MsgHeartbeat:
	case pb.MsgSnap:
	case myVoteRespType:
	case pb.MsgTimeoutNow:
	}
}

func stepFollower(r *raft, m pb.Message) {
	switch m.Type {
	case pb.MsgProp:
	case pb.MsgApp:
	case pb.MsgHeartbeat:
	case MsgSnap:
	case MsgTransferLeader:
	case pb.MsgTimeoutNow:
	case pb.MsgReadIndex:
	case pb.MsgReadIndexResp:
	}
}