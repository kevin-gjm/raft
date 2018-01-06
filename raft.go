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
	if err := c.validate();err != nil {
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
		raftLog:			raftlog,
		maxMsgSize:			c.MaxSizePerMsg,
		maxInflight:		c.MaxInflightMsgs,
		prs:				make(map[uint64]*Progress),
		learnerPrs:			make(map[uint64]*Progress),
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
		r.prs[p] = &Progress{Next:1,ins:newInflights(r.maxInflight)}
	}
	for _,p :=range learners {
		if _,ok := r.prs[p];ok {
			panic(fmt.Sprintf("node %x is in both learn and peer list",p))
		}
		r.learnerPrs[p] = &Progress{Next:1,ins:newInflights(r.maxInflight),IsLearner:true}
		if r.id == p {
			r.isLearner = true
		}
	}

	//载入hardState
	if !isHardStateEqual(hs,emptyState) {
		r.loadState(hs)
	}
	//读取日志中的相关信息
	if c.Applied >0 {
		raftlog.appliedTo(c.Applied)
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

func (r *raft) sendHeartbeat(to uint64,ctx []byte) {
	//跟随者也许跟领导者不同步，或者不含有所有的提交日志，领导者必须不能发送commit将跟随者的commit向前推进
	commit := min(r.getProgress(to).Match,r.raftLog.committed)

	m:= pb.Message{
		To:	to,
		Type: pb.MsgHeartbeat,
		Commit:commit,
		Context:ctx,
	}
	r.send(m)
}

//raft的每一个peer都自信参数中的函数
func (r *raft)forEachProgress(f func(id uint64,pr *Progress)) {
	for id,pr := range r.prs {
		f(id,pr)
	}

	for id,pr := range r.learnerPrs {
		f(id,pr)
	}
}

func (r *raft)bcastAppend() {
	r.forEachProgress( func(id uint64, _ *Progress){
		if id == r.id {
			return 
		}
		r.sendAppend(id)
	})
}
//根据对端progress决定添加多少log entry，并发送到对端
func(r *raft) bcastHeartbeat() {
	lastCtx := r.readOnly.lastPendingRequestCtx()
	if len(lastCtx) == 0 {
		r.bcastHeartbeatWithCtx(nil)
	}else{
		r.bcastHeartbeatWithCtx([]byte(lastCtx))
	}
}

func (r *raft) bcastHeartbeatWithCtx(ctx []byte) {
	r.forEachProgress(func(id uint64,_ *Progress){
		if id == r.id {
			return 
		}
		r.sendHeartbeat(id,ctx)
	})
}
//试图推进commit index。成功返回true
//获取所有的progress的commit match，然后排序。获取多数人位置处的提交index.此位置即为新的committed  index
func (r *raft) maybeCommit() bool {
	mis := make(uint64Slice,0,len(r.prs))
	for _,p := range r.prs {
		mis = append(mis,p.Match)
	}
	sort.Sort(sort.Reverse(mis))
	//多数的提交index
	mci := mis[r.quorum() -1]
	return r.raftLog.maybeCommit(mci,r.Term)
}
func (r *raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()

	r.abortLeaderTransfer()

	r.votes = make(map[uint64]bool)
	r.forEachProgress(func(id uint64,pr *Progress) {
		*pr = Progress{Next:r.raftLog.lastIndex() +1, ins:newInflights(r.maxInflight),IsLearner:pr.IsLearner}
		if id == r.id {
			pr.Match = r.raftLog.lastIndex()
		}
	})

	r.pendingConf = false
	r.readOnly = newReadOnly(r.readOnly.option)
}

func (r *raft) appendEntry(es ...pb.Entry) {
	li := r.raftLog.lastIndex()
	for i:= range es {
		es[i].Term = r.Term
		es[i].Index = li + 1 +uint64(i)
	}

	r.raftLog.append(es...)
	r.getProgress(r.id).maybeUpdate(r.raftLog.lastIndex())
	r.maybeCommit()
}

//非领导者选举超时后，执行此函数.每一个tick执行一次，计数处理判断超时
func(r *raft)tickElection() {
	r.electionElapsed++
	if r.promotable() && r.pastElectionTimeout() {
		r.electionElapsed = 0
		r.Step(pb.Message{From:r.id,Type:pb.MsgHup})
	}
}

func (r *raft) tickHeartbeat() {
	r.heartbeatElapsed++
	r.electionElapsed++ 
	
	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0 
		if r.checkQuorum {
			r.Step(pb.Message{From:r.id,Type:pb.MsgCheckQuorum})
		}
		if r.state == StateLeader && r.leadTransferee != None {
			r.abortLeaderTransfer()
		}
	}
	if r.state != StateLeader {
		return 
	} 

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{From:r.id,Type:pb.MsgBeat})
	}
}

func(r *raft) becomeFollower(term uint64,lead uint64) {
	r.step = stepFollower
	r.reset(term)
	r.tick=r.tickElection
	r.lead = lead
	r.state = StateFollower
	r.logger.Infof("%x became follower at term %x",r.id,r.Term)
}

func(r *raft) becomeCandidate() {
	if r.state == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.step = stepCandidate
	r.reset(r.Term+1)
	r.tick = r.tickElection
	r.Vote = r.id
	r.state = StateCandidate
	r.logger.Infof("%x become candidate at term %d",r.id,r.Term)
}

func (r *raft) becomePreCandidate() {
	if r.state == StateLeader {
		panic("invalid transition [leader -> pre-candidate]")
	}
	r.step = stepCandidate
	r.votes = make(map[uint64]bool)
	r.tick = r.tickElection
	r.state = StatePreCandidate
	r.logger.Infof("%x become pre_candidate at term %d",r.id,r.Term)
}

func (r *raft) becomeLeader() {
	if r.state == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.step = stepLeader
	r.reset(r.Term)
	r.tick = r.tickHeartbeat
	r.lead = r.id
	r.state = StateLeader

	ents,err := r.raftLog.entries(r.raftLog.committed+1,noLimit)
	if err != nil {
		r.logger.Panicf("unexpected error getting uncommitted entries (%v)",err)
	}
	nconf := numOfPendingConf(ents)
	if nconf > 1 {
		panic("unexpected multiple uncommitted config entry")
	}
	if nconf == 1 {
		r.pendingConf = true
	}
	r.appendEntry(pb.Entry{Data:nil})
	r.logger.Infof("%x become leader at term %d",r.id,r.Term)
}

func(r *raft) campaign(t CampaignType) {
	var term uint64
	var voteMsg pb.MessageType

	if t == campaignPreElection {
		r.becomePreCandidate()
		voteMsg = pb.MsgPreVote
		//preVote rpc send for the next term
		term = r.Term + 1
	} else {
		r.becomeCandidate()
		voteMsg = pb.MsgVote
		term = r.Term
	}

	if r.quorum() == r.poll(r.id,voteRespMsgType(voteMsg),true) {
		//参加选举成功，进入下一个状态
		if t == campaignPreElection {
			r.campaign(campaignElection)
		}else{
			r.becomeLeader()
		}
		return
	}

	for id := range r.prs {
		if id == r.id {
			continue
		}
		r.logger.Infof("%x [logterm: %d,index: %d] sent %s request to %x at term %d",
			r.id,r.raftLog.lastTerm(),r.raftLog.lastIndex(),voteMsg,id,r.Term)
		var ctx []byte
		if t == campaignTransfer {
			ctx = []byte(t)
		}
		r.send(pb.Message{Term:term,To:id,Type:voteMsg,Index:r.raftLog.lastIndex(),LogTerm:r.raftLog.lastTerm(),Context:ctx})
	}
}

func (r *raft) poll( id uint64,t pb.MessageType,v bool) (granted int) {
	if v {
		r.logger.Infof("%x received %s from %x at term %d",r.id,t,id,r.Term)
	}else {
		r.logger.Infof("%x received %s rejection from %x at term %d",r.id,t,id,r.Term)
	}
	if _,ok := r.votes[id]; !ok {
		r.votes[id] = v
	}

	for _,vv :=range r.votes {
		if vv {
			granted++
		}
	}
	return granted
}

func (r *raft) Step(m pb.Message) error {
	switch {
	case m.Term==0:
		//local message
	case m.Term > r.Term:
		if m.Type == pb.MsgVote || m.Type == pb.MsgPreVote {
			force := bytes.Equal(m.Context,[]byte(campaignTransfer))
			inLease := r.checkQuorum && r.lead != None && r.electionElapsed<r.electionTimeout

			if !force && inLease {
				//若在最小的选举时间内收到vote的请求。忽略请求
				r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: lease is not expired (remaining ticks: %d)",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term, r.electionTimeout-r.electionElapsed)
			}
		}
		switch {
		case m.Type == pb.MsgPreVote:
			//不更改term
		case m.Type == pb.MsgPreVoteResp && !m.Reject :
			//当pre-vote被大多数确认后，我们将更改我们的term.若非如此term是拒绝我们的，下一个term变成跟随者
		default:
			//收到更大的term，身份变更为跟随者
			r.logger.Infof("%x [term: %d] received a %s message with higher term from %x [term： %d]",
				r.id,r.Term,m.Type,m.From,m.Term)
				if m.Type == pb.MsgApp || m.Type == pb.MsgHeartbeat || m.Type == pb.MsgSnap {
					r.becomeFollower(m.Term,m.From)
				}else {
					r.becomeFollower(m.Term,None)
				}		
		}
	case m.Term < r.Term:
		if r.checkQuorum && (m.Type == pb.MsgHeartbeat || m.Type == pb.MsgApp) {
			//我们收到一个leader发来的lower term信息，这有可能是网络延时造成的，也可能是此节点在之前网络分隔的情况下推进了term
			r.send(pb.Message{To: m.From,Type:pb.MsgAppResp}) //忽略信息内容，将此本节点的term返回给他
		}else {
			//忽略其他信息
			r.logger.Infof("%x [term: %d] ignore a %s msg with lower term from %x [term: %d]",
				r.id,r.Term,m.From,m.Term)
		}
		return nil
	}


	switch m.Type {
	case pb.MsgHup://自己要参加选举
		if r.state != StateLeader {
			ents,err := r.raftLog.slice(r.raftLog.applied+1,r.raftLog.committed+1,noLimit)
			if err != nil {
				r.logger.Panicf("unexpected error getting unapplied entries (%v)",err)
			}
			if n:=numOfPendingConf(ents);n!= 0 && r.raftLog.committed > r.raftLog.applied {
				//有n个配置更改日志，不允许参加选举
				r.logger.Warningf("%x cannot campaign at term %d since there are still % pending configure changes to apply",r.id,r.Term,n)
				return nil
			}

			r.logger.Infof("%x is starting a new election at term %d",r.id,r.Term)
			if r.preVote {
				r.campaign(campaignPreElection)
			}else{
				r.campaign(campaignElection)
			}
		}else {
			r.logger.Infof("%x ignore MsgHup because alread leader",r.id)
		}
	case pb.MsgVote,pb.MsgPreVote:
		if r.isLearner {
			//忽略
			r.logger.Infof("%x [logterm: %d,index: %d, vote: %x] ignore %s from %x [logterm: %d,index: %d] at term %d;learner can not vote",
				r.id,r.raftLog.lastTerm(),r.raftLog.lastIndex(),r.Vote,m.Type,m.From,m.LogTerm,m.Index,r.Term)
			return nil
		}
		// m.term > r.Term prevote时产生。对msgvote m.term==r.term总是相等
		//isUpToDate 给定的index和term比log中所有内容都新
		if (r.Vote== None || m.Term > r.Term||r.Vote == m.From) && r.raftLog.isUpToDate(m.Index,m.LogTerm) {
			//投票
			r.logger.Infof("%x [logterm: %d,index: %d,vote: %x] cast %s for %x [logterm: %d,index: %d] at term %d",
				r.id,r.raftLog.lastTerm(),r.raftLog.lastIndex(),r.Vote,m.Type,m.From,m.Term,m.Index,r.Term)
			//使用收到消息中的term进行回复(非本地的term)
			r.send(pb.Message{To:m.From,Term:m.Term,Type:voteRespMsgType(m.Type)})
			if m.Type == pb.MsgVote {
				r.electionElapsed = 0
				r.Vote = m.From
			}
		}else {
			//拒绝投票
			r.logger.Infof("%x [logterm: %d,index: %d,vote: %x] reject %s from %x [logterm: %d,index:%d] at term %d",
				r.id,r.raftLog.lastTerm(),r.raftLog.lastIndex(),r.Vote,m.Type,m.From,m.Term,m.Index,r.Term)
			r.send(pb.Message{To:m.From,Term:r.Term,Type:voteRespMsgType(m.Type),Reject:true})
		}
	default:
		r.step(r,m)
	}
	return nil
}
type stepFunc func(r *raft, m pb.Message)

// 领导者对消息的处理过程
func stepLeader(r *raft,m pb.Message) {
	switch m.Type {
	case pb.MsgBeat:
		r.bcastHeartbeat()
		return
	case pb.MsgCheckQuorum:
		//支持者中大多数都挂了，肯定不能继续当领导者了
		if !r.checkQuorumActive() {
			r.logger.Warningf("%x stepped down to follower since quorum is not active",r.id)
			r.becomeFollower(r.Term,None)
		}
	case pb.MsgProp:
		if len(m.Entries) == 0 {
			r.logger.Panicf("%x stepped empty MsgProp",r.id)
		}
		if _,ok := r.prs[r.id];!ok {
			//若我们不是range中的一个(如：作为领导时将自己移除)
			return 
		}
		if r.leadTransferee != None {
			r.logger.Debugf("%x [term %d] transfer leadership to %x is in progress;droping proposal",r.id,r.Term,r.leadTransferee)
			return 
		}
		for i,e := range m.Entries {
			if e.Type == pb.EntryConfChange {
				if r.pendingConf {
					r.logger.Infof("propose conf %s ignored since pending unapplied configuration",e.String())
					m.Entries[i] = pb.Entry{Type:pb.EntryNormal}
				}
				r.pendingConf = true
			}
		}
		r.appendEntry(m.Entries...)
		r.bcastAppend()
		return
	case pb.MsgReadIndex:
		if r.quorum() > 1 {
			if r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(r.raftLog.committed)) != r.Term {
			//拒绝此次请求此领导者在此term内有未提交的log
			return 
			}

			switch r.readOnly.option {
			case ReadOnlySafe:
				r.readOnly.addRequest(r.raftLog.committed,m)
				r.bcastHeartbeatWithCtx(m.Entries[0].Data)
			case ReadOnlyLeaseBased:
				ri := r.raftLog.committed
				if m.From == None || m.From == r.id {
				//来自local member
					r.readStates = append(r.readStates, ReadState{Index: r.raftLog.committed, RequestCtx: m.Entries[0].Data})
				}else {
					r.send(pb.Message{To: m.From,Type:pb.MsgReadIndexResp,Index:ri,Entries:m.Entries})
				}
			}
		}else {
		r.readStates = append(r.readStates,ReadState{Index: r.raftLog.committed, RequestCtx: m.Entries[0].Data})
		}
	}

	//所有其他的消息类型需要一个progress来进行
	pr := r.getProgress(m.From)
	if pr == nil {
		r.logger.Debugf("%x no progress available for %x",r.id,m.From)
	}

	switch m.Type {
	case pb.MsgAppResp:
		pr.RecentActive = true

		if m.Reject {
			r.logger.Debugf("%x received msgApp rejection(lastindex: %d) from %x from index %d",
				r.id,m.RejectHint,m.From,m.Index)
			if pr.maybeDecrTo(m.Index,m.RejectHint) {
				//重新设置完成peer的progress
				r.logger.Debugf("%x decreased progress of %x to [%s]",r.id,m.From,pr)
				if pr.State == ProgressStateReplicate {
					pr.becomeProbe()
				}
				r.sendAppend(m.From)
			}
		}else {
			odlPaused := pr.IsPaused()
			if pr.maybeUpdate(m.Index) {
				//对端处理消息成功更新pr的状态并更新pr中的inflights
				switch{
				case pr.State == ProgressStateProbe:
					pr.becomeReplicate()
				case pr.State == ProgressStateSnapshot && pr.needSnapshotAbort() :
					r.logger.Debugf("%x snapshot aborted, resumed sending replication message to %x [%s]",r.id,m.From,pr)
					pr.becomeProbe()
				case pr.State == ProgressStateReplicate:
					pr.ins.freeTo(m.Index)
				}

				if r.maybeCommit() {
					r.bcastAppend()
				}else if odlPaused {
					//snapshot中暂停处理。此处为一个恢复点，另一个是heartbeatresp
					r.sendAppend(m.From)
				}
				if m.From == r.leadTransferee && pr.Match == r.raftLog.lastIndex() {
					r.logger.Infof("%x sent MsgTimeoutNow to %x after received MsgAppResp",r.id,m.From)
					r.sendTimeoutNow(m.From)
				}
			}
		}
	case pb.MsgHeartbeatResp:
		pr.RecentActive = true
		pr.resume()

		//从满的inflights中释放一个位置，允许进行progress
		if pr.State == ProgressStateReplicate && pr.ins.full() {
			pr.ins.freeFirstOne()
		}
		if pr.Match < r.raftLog.lastIndex() {
			r.sendAppend(m.From)
		}

		if r.readOnly.option != ReadOnlySafe || len(m.Context) == 0 {
			return
		}
		ackCount := r.readOnly.recvAck(m)
		if ackCount < r.quorum() {
			return
		}

		//返回以m为ctx的所有的readindex
		rss := r.readOnly.advance(m)

		for _,rs := range rss {
			req := rs.req  //相应的消息
			if req.From == None || req.From == r.id {
				//添加到readStates 留待处理
				r.readStates = append(r.readStates,ReadState{Index:rs.index,RequestCtx:req.Entries[0].Data})
			}else {
				//其他节点将readindex相关的信息发送给对端
				r.send(pb.Message{To:req.From,Type:pb.MsgReadIndexResp,Index:rs.index,Entries:req.Entries})
			}
		}

	case pb.MsgSnapStatus:
		if pr.State != ProgressStateSnapshot{
			return
		}
		if !m.Reject {
			pr.becomeProbe()
			r.logger.Debugf("%x snapshot succeeded,resumed send replication message to %x [%s]",r.id,m.From,pr)
		}else {
			pr.snapshotFailure()
			pr.becomeProbe()
			r.logger.Debugf("%x snapshot failed,resumed sending replication message to %x [%s]",r.id,m.From,pr)
		}
		//若snapshot失败，等待下一个heartbeat
		//snapshot完成，等待下一个msgApp
		pr.pause()
	case pb.MsgUnreachable:
		//正常的过程中，若unreachable，肯达可能是a MsgApp is lost
		if pr.State == ProgressStateReplicate {
			pr.becomeProbe()
		}
		r.logger.Debugf("%x failed to send message to %x because it is unreachable [%s]",r.id,m.From,pr)
	case pb.MsgTransferLeader:
		if pr.IsLearner {
			r.logger.Debugf("%x is learner. Ignore transferring leadership",r.id)
			return 
		}
		leadTransferee := m.From
		lastLeadTransferee := r.leadTransferee 
		if lastLeadTransferee != None {
			if lastLeadTransferee == leadTransferee {
				r.logger.Infof("%x [term %d] transfer leadership to %x is in progress,ignore request to same node %x",
					r.id,r.Term,leadTransferee,leadTransferee)
				return
			}
			r.abortLeaderTransfer()
			r.logger.Infof("%x [term %d] abort previous transferring leadership to %x",r.id,r.Term,lastLeadTransferee)
		}
		if leadTransferee == r.id {
			r.logger.Debugf("%x is already leader. Ignore transferring leadership to self",r.id)
			return
		}
		r.logger.Infof("%x [term %d] starts to transfer leadership to %x", r.id, r.Term, leadTransferee)
		//领带更迭需要在一个选举周期中完成，因此重置 r.electionElapsed.
		r.electionElapsed =0
		r.leadTransferee = leadTransferee

		if pr.Match == r.raftLog.lastIndex() {
			r.sendTimeoutNow(leadTransferee)
			r.logger.Infof("%x sends MsgTimeoutNow to %x immediately as %x already has up-to-date log",r.id,leadTransferee,leadTransferee)
		}else {
			r.sendAppend(leadTransferee)
		}
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
	case pb.MsgSnap:
	case pb.MsgTransferLeader:
	case pb.MsgTimeoutNow:
	case pb.MsgReadIndex:
	case pb.MsgReadIndexResp:
	}
}



func (r *raft) promotable() bool {
	_,ok := r.prs[r.id]
	return ok
}
func (r *raft) loadState(state pb.HardState) {
	if state.Commit < r.raftLog.committed || state.Commit > r.raftLog.lastIndex() {
		r.logger.Panicf("%x state.commit %d is out of range [%d,%d]",r.id,state.Commit,r.raftLog.committed,r.raftLog.lastIndex())
	}
	r.raftLog.committed = state.Commit
	r.Term = state.Term
	r.Vote = state.Vote
}
//若r.electionElapsed比election timeout >= 返回true 
func (r *raft) pastElectionTimeout() bool {
	return r.electionElapsed >= r.randomizedElectionTimeout
}

func (r *raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

//当前raft状态机中包含本机在内的所有机器progress中recentactive状态的和 >= 大多数 -->return true
//此函数还会把所有的recentactive设置为false
func (r *raft) checkQuorumActive() bool {
	var act int

	r.forEachProgress(func(id uint64,pr *Progress){
		if id == r.id {
			act++
		}
		if pr.RecentActive && !pr.IsLearner {
			act++
		}
		pr.RecentActive = false
	})
	return act >= r.quorum()
}
func(r *raft) sendTimeoutNow(to uint64) {
	r.send(pb.Message{To:to,Type:pb.MsgTimeoutNow})
}

func (r *raft) abortLeaderTransfer() {
	r.leadTransferee = None
}

func numOfPendingConf(ents []pb.Entry) int {
	n:=0
	for i:= range ents {
		if ents[i].Type == pb.EntryConfChange {
			n++
		}
	}
	return n
}