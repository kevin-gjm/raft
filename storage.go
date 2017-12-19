package raft 

import (
	"errors"
	"sync"
	pb "github.com/coreos/etcd/raft/raftpb"
)

// id早于压缩的id
var ErrCompacted = errors.New("requested index is unavailable due to compaction") 
var ErrSnapOutOfDate = errors.New("requested index is older than the existing snapshot")
var ErrUnavailable = errors.New("requested entry at index is unavailable")
var ErrSnapshotTemporarilyUnavailable = errors.New("snapshot is temporarily unavailable")

// 如果任意一个storage中函数返回错误。raft变的不操作不能重新参加选举。程序有责任清理并恢复
type Storage interface {
	//返回存储中的hardstat 和 conf
	InitialState() (pb.HardState,pb.ConfState,error)
	//返回[lo,hi)之间的数据，最大maxsize.此size指的是占用空间的大小
	Entries(lo,hi,maxSize uint64) ([]pb.Entry,error)
	//返回entry i的term， i有范围限制，需要在 [FirstIndex()-1, LastIndex()]。
	//FirstIndex()-1保留是为了匹配而进行使用.FirstIndex()-1存储在 entry的[0]中，用来标记snap中的状态等信息
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

func (ms *MemoryStorage) Term(i uint64) (uint64,error) {
	ms.Lock()
	defer ms.Unlock()

	offset := ms.ents[0].Index
	if i < offset {
		return 0,ErrCompacted
	}
	if int(i-offset) >=len(ms.ents) {
		return 0,ErrUnavailable
	}
	return ms.ents[i-offset].Term,nil
}

func (ms *MemoryStorage) LastIndex() (uint64,error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.lastIndex(),nil
}

func (ms *MemoryStorage) lastIndex() uint64 {
	return ms.ents[0].Index +uint64(len(ms.ents))-1
}

func (ms *MemoryStorage) FirstIndex()(uint64,error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.firstIndex(),nil
}
func (ms *MemoryStorage) firstIndex() uint64 {
	return ms.ents[0].Index+1
}

func (ms *MemoryStorage) Snapshot() (pb.Snapshot,error){
	ms.Lock()
	defer ms.Unlock()
	return ms.snapshot,nil
}
//应用快照，首先判断快照中的index是否大于原来snap中的index。
//应用快照之后将ents中的数据清空，并将snap metadata中的信息写入 entry的[0]条数据中
func (ms *MemoryStorage) ApplySnapshot(snap pb.Snapshot) error {
	ms.Lock()
	defer ms.Unlock()

	msIndex := ms.snapshot.Metadata.Index
	snapIndex := snap.Metadata.Index

	if msIndex >= snapIndex {
		return ErrSnapOutOfData
	}
	ms.snapshot = snap
	ms.ents = []pb.Entry{{Term:snap.Metadata.Term,Index:snap.Metadata.Index}}
	return nil
}

//参数i (index) 是代表i之前的数据做快照处理。data是存储在快照中的数据data??
func (ms *MemoryStorage) CreateSnapshot(i uint64,cs *pb.ConfState,data []byte) (pb.Snapshot,error){
	ms.Lock()
	defer ms.Unlock()

	if i <= ms.snapshot.Metadata.Index {
		return pb.Snapshot{},ErrSnapOutOfData 
	}
	offset := ms.ents[0].Index
	if i > offset+ms.lastIndex() {
		raftLogger.Panicf("snapshot %d is out of bound lastindex(%d)",i,ms.lastIndex())
	}

	ms.snapshot.Metadata.Index = i
	ms.snapshot.Metadata.Term = ms.ents[i-offset].Term
	if cs != nil {
		ms.snapshot.Metadata.ConfState = *cs
	}
	ms.snapshot.Data = data
	return ms.snapshot,nil
}

//压缩：会丢弃compactIndex之前的log entries。保证压缩index大于应用id(applied)是应用程序的责任
func (ms *MemoryStorage) Compact(compactIndex uint64) error {
	ms.Lock()
	defer ms.Unlock()

	offset := ms.ents[0].Index

	if compactIndex <= offset {
		return ErrCompacted
	}
	if compactIndex > offset+ms.lastIndex() {
		raftLogger.Panicf("campact %d is out of bound lastindex(%d)",compactIndex,ms.lastIndex())
	}

	i := compactIndex - offset
	ents:= make([]pb.Entry,1,1+uint64(len(ms.ents))-i)
	ents[0].Index = ms.ents[i].Index
	ents[0].Term = ms.ents[i].Term
	ents = append(ents,ms.ents[i+1:]...)
	ms.ents = ents
	return nil
}

func (ms *MemoryStorage) Append(entries []pb.Entry) error   {
	if len(entries) == 0 {
		return nil
	}
	ms.Lock()
	defer ms.Unlock()

	first := ms.firstIndex()
	last := entries[0].Index + uint64(len(entries)) - 1

	//已经添加了
	if last < first {
		return nil
	}
	//截断数据，之前的已经在相应的快照中了
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}

	offset := entries[0].Index - ms.ents[0].Index
	switch {
	case uint64(len(ms.ents)) > offset:
		//部分添加,entries和ms.ents中一部分是重复的
		ms.ents = append([]pb.Entry{},ms.ents[:offset]...)
		ms.ents = append(ms.ents,entries...)
	case uint64(len(ms.ents)) == offset:
		//正好卡上，直接添加
		ms.ents = append(ms.ents,entries...)
	default:
		//ms.ents和entries之间空出来数据了
		raftLogger.Panicf("missing log entry[last:%d,append:%d]",ms.lastIndex(),entries[0].Index)
	}
	return nil
}