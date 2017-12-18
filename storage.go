package raft 

import (
	"errors"
	"sync"
	pb "github.com/coreos/etcd/raft/raftpb"
)

// id早于压缩的id
var ErrCompacted = errors.New("requested index is unavailable due to compaction") 
var ErrSnapOutOfData = errors.New("requested index is older than the existing snapshot")
var ErrUnavailable = errors.New("requested entry at index is unavailable")
var ErrSnapshotTemporarilyUnavailable = errors.New("snapshot is temporarily unavailable")

// 如果任意一个storage中函数返回错误。raft变的不操作不能重新参加选举。程序有责任清理并恢复
type Storage interface {
	//返回存储中的hardstat 和 conf
	InitialState() (pb.HardState,pb.ConfState,error)
	//返回[lo,hi)之间的数据，最大maxsize
	Entries(lo,hi,maxSize uint64) ([]pb.Entry,error)
	//返回entry i的term， i有范围限制，需要在 [FirstIndex()-1, LastIndex()]。
	//FirstIndex()-1保留是为了匹配而进行使用
	Term(i uint64) (uint64,error)
	//返回log中最后一条的index
	LastIndex() (uint64,error)
	//log中的第一条的index，之前的index可能已经存入到snap中了
	FirstIndex() (uint64,error)
	//返回最近的快照，若snap暂时不可用，返回ErrSnapshotTemporarilyUnavailable，
	//这样raft就知道Storage需要时间来准备快照，之后再次尝试即可
	Snapshot() (pb.Snapshot,error)
}

//在内存中用数组实现Storage接口的存储
type MemoryStorage struct {
	sync.Mutex

	hardState pb.HardState
	snapshot  pb.Snapshot
	//ents[i] has raft log position i+snapshot.Metadata.Index
	ents []pb.Entry
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage {
		//第0项，使用虚拟条目进行填充
		ents:make([]pb.Entry,1),
	}
}

func (ms *MemoryStorage) InitialState() (pb.HardState,pb.ConfState,error) {
	return ms.hardState,ms.snapshot.Metadata.ConfState,nil
}

func (ms *MemoryStorage) SetHardState(st pb.HardState) error {
	ms.Lock()
	defer ms.Unlock()
	ms.hardState = st
	return nil
}

func (ms *MemoryStorage) Entries(lo,hi,maxSize uint64) ([]pb.Entry,error) {
	ms.Lock()
	defer ms.Unlock()

	offset := ms.ents[0].Index
	if lo <= offset {
		return nil,ErrCompacted
	}
	if hi > ms.lastIndex()+1 {
		raftLogger.Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, ms.lastIndex())
	}
	//只有第0条，虚拟的条目
	if len(ms.ents) == 1 {
		return nil,ErrUnavailable
	}
	ents := ms.ents[lo-offset:hi-offset]

	return limitSize(ents,maxSize),nil
}
func (ms *MemoryStorage) LastIndex() (uint64,error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.lastIndex(),nil
}
func (ms *MemoryStorage) lastIndex() uint64 {
	return ms.ents[0].Index +uint64(len(ms.ents))-1
}