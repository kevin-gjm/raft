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