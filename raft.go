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
	if c.ReadOnlyOption = ReadOnlyLeaseBased && !c.CheckQuorum {
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
type stepFunc func(r *raft, m pb.Message)