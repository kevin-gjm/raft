package raft

import (
	"context"
	"errors"

	pb "github.com/coreos/etcd/raft/raftpb"
)

type SnapshotStatus int 

const (
	SnapshotFinish  SnapshotStatus = 1
	SnapshotFailure SnapshotStatus = 2
)

var (
	emptyState	= pb.HardState{}
	ErrStopped	= errors.New("raft: stopped")
)
//对写日志和调试非常好用
type SoftState struct {
	Lead      uint64 //必须原子访问
	RaftState StateType
}

func (a *SoftState) equal(b *SoftState) bool {
	return a.Lead ==b.Lead && a.RaftState == b.RaftState
}

//READY 顾名思义，是否可用。所有的数据字段都是只读的
// 封装了可用、要落盘、提交、发送的 entries和messages 
// 有消息是否可用,又提交是否可用，有更改、同步等
type Ready struct {
	//没有更新为nil
	*SoftState
	//在消息发送之前要落盘的节点状态信息
	//没有更新为nil
	pb.HardState

	ReadStates []ReadState

	Entries []pb.Entry

	Snapshot pb.Snapshot

	CommittedEntries []pb.Entry

	Messages []pb.Message

	MustSync bool
}

func isHardStateEqual(a,b pb.HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}

func IsEmptyHardState(st pb.HardState) bool {
	return isHardStateEqual(st,emptyState)
}

func IsEmptySnap(sp pb.Snapshot) bool {
	return sp.Metadata.Index == 0
}

//接收类型非指针内部状态不可更改
//内部数据是否可以对外部提供服务，READY
func (rd Ready) containsUpdates() bool {
	return rd.SoftState != nil || IsEmptyHardState(rd.HardState) ||
	!IsEmptySnap(rd.Snapshot) || len(rd.Entries) > 0 ||
	len(rd.CommittedEntries) >0 || len(rd.Messages) >0 || len(rd.ReadStates) != 0
}

type Node interface {
	Tick()
	//竞选
	Campaign(ctx context.Context) error
	//提议
	Propose(ctx context.Context,data []byte) error
	//
	ProposeConfChange(ctx context.Context, cc pb.ConfChange) error
	//状态机应用msg.
	Step(ctx context.Context,msg pb.Message) error 

	Ready() <-chan Ready

	//标志着应用程序上次Ready已经执行完成，让node返回下一个Ready
	//当应用完上一条Ready时一般会调用此函数

	//作为优化，可以在应用上一个ready时调用次函数。如：应用snap需要很长时间，
	//为了继续接收ready不阻塞raft过程。可以同步进行
	Advance()

	ApplyConfChange(cc pb.ConfChange) * pb.ConfState

	//将leadership传递给transferee
	TransferLeadership(ctx context.Context,lead,transferee uint64)

	//请求一个read state.read state存在ready中
	//read state 有一个index，客户端index比他早，所有的线性读取都是安全的
	ReadIndex(ctx context.Context, rctx[] byte) error

	Status() Status

	//报告给定node上次发送不可达
	ReportUnreachable(id uint64)

	//报告发送snap状态
	ReportSnapshot(id uint64,status SnapshotStatus)

	//处理Node的任意的终止信号
	Stop()

}

type Peer struct {
	ID  uint64
	Context []byte
}

func StartNode(c *Config,peers []Peer) Node {
	r := newRaft(c)
}