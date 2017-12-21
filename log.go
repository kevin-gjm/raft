package raft

import (
	"fmt"
	//"math"

	pb "github.com/coreos/etcd/raft/raftpb"
)

//const noLimit = math.MaxUint64

type raftLog struct {
	//存储所有log since the last snapshot
	storage Storage
	//存储所有unstable数据和snap，之后会保存到storage中
	unstable unstable
	//大多数节点上已经提交的最大id
	committed uint64
	//应用程序应用到自己状态机中的最大id， applied<= committed
	applied uint64

	logger Logger
}

// 新的log用给定的storage恢复，commit和applied恢复到最后压缩的状态。
//TODO
// ？？好像是其他的log存储在entries数组中，不能持久化
func newLog(storage Storage, logger Logger) *raftLog {
	if storage == nil {
		logger.Panic("storage must not be nil")
	}

	log := &raftLog{
		storage: storage,
		logger:  logger,
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	log.unstable.offset = lastIndex + 1
	log.unstable.logger = logger
	//将commited和applied置为上次压缩时的状态
	//？？？？
	log.committed = firstIndex - 1
	log.applied = firstIndex - 1

	return log
}

func (l *raftLog) String() string {
	return fmt.Sprintf("commited=%d,applied=%d,unstable.offset=%d,len(unstable.Entries)=%d", l.committed, l.applied, l.unstable.offset, len(l.unstable.entries))
}

// 若添加失败，返回(0,false)。成功返回(last index of new entries,true)
// 若添加的ents中和commited(含)之前的数据冲突，失败
// 若添加的所有log都在commited之前，则减小commited到ents最后一条
func (l *raftLog) maybeAppend(index, logTerm, commited uint64, ents ...pb.Entry) (lastnewi uint64, ok bool) {
	if l.matchTerm(index, logTerm) {
		lastnewi = index + uint64(len(ents))
		ci := l.findConflict(ents)
		switch {
		case ci == 0:
		case ci <= l.committed:
			l.logger.Panicf("entry %d conflict with commited entry [commited(%v)]", ci, l.committed)
		default:
			//冲突点在commited之后用新的ents冲突点之后的数据覆盖原先的数据
			offset := index + 1
			l.append(ents[ci-offset:]...)
		}
		//TODO
		//TODO
		//需要在这个地方更新commited么？？？？
		// commited 在此处可能会减小。
		l.commitTo(min(commited, lastnewi))
		return lastnewi, true
	}
	return 0, false
}
func (l *raftLog) append(ents ...pb.Entry) uint64 {
	if len(ents) == 0 {
		return l.lastIndex()
	}
	if after := ents[0].Index - 1; after < l.committed {
		l.logger.Panicf("after(%d) is out of range [commited(%d)]", after, l.committed)
	}
	l.unstable.truncateAndAppend(ents)
	return l.lastIndex()
}

//有冲突，返回冲突点index
//无冲突，raft已经包含有全部的日志，返回0
//无冲突，ents中含有raft中没有的日志，返回第一条新的index
func (l *raftLog) findConflict(ents []pb.Entry) uint64 {
	for _, ne := range ents {
		//重合部分检测是否有冲突，若有冲突找到冲突点并返回
		if !l.matchTerm(ne.Index, ne.Term) {
			if ne.Index <= l.lastIndex() {
				l.logger.Infof("found conflicit at index %d [existing term:%d,conflict term %d]",
					ne.Index, l.zeroTermOnErrCompacted(l.term(ne.Index)), ne.Term)
			}
			return ne.Index
		}
	}
	return 0
}
func (l *raftLog) unstableEntries() []pb.Entry {
	if len(l.unstable.entries) == 0 {
		return nil
	}
	return l.unstable.entries
}

//返回所有applied之后的可以进行应用的entries
//如果applied比snap.index小，则返回snap之后所有的committed
func (l *raftLog) nextEnts() (ents []pb.Entry) {
	off := max(l.applied+1, l.firstIndex())
	if l.committed+1 > off {
		ents, err := l.slice(off, l.committed+1, noLimit)
		if err != nil {
			l.logger.Panicf("unexpected error when getting unapplied entries (%v)", err)
		}
		return ents
	}
	return nil
}

//是否存在用于apply的entries
func (l *raftLog) hasNextEnts() bool {
	off := max(l.applied+1, l.firstIndex())
	return l.committed+1 > off
}

func (l *raftLog) snapshot() (pb.Snapshot, error) {
	if l.unstable.snapshot != nil {
		return *l.unstable.snapshot, nil
	}
	return l.storage.Snapshot()
}

func (l *raftLog) firstIndex() uint64 {
	if i, ok := l.unstable.maybeFirstIndex(); ok {
		return i
	}
	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	return index
}

func (l *raftLog) lastIndex() uint64 {
	if i, ok := l.unstable.maybeLastIndex(); ok {
		return i
	}
	index, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return index
}

func (l *raftLog) commitTo(tocommit uint64) {
	if l.committed < tocommit {
		if l.lastIndex() < tocommit {
			l.logger.Panicf("to commit(%d) is out of range [lastIndex(%d)]. was the raft log corrupted, truncated, or lost?", tocommit, l.lastIndex())
		}
		l.committed = tocommit
	}
}
func (l *raftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}

	if l.committed < i || i < l.applied {
		l.logger.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	l.applied = i
}

//干掉index、term对应数据之前的数据
func (l *raftLog) stableTo(i, t uint64) { l.unstable.stableTo(i, t) }

//index与unstable.snap中的index相同干掉 unstable的snap
func (l *raftLog) stableSnapTo(i uint64) { l.unstable.stableSnapTo(i) }

func (l *raftLog) lastTerm() uint64 {
	t, err := l.term(l.lastIndex())
	if err != nil {
		l.logger.Panicf("unexpected err when getting the last term (%v)", err)
	}
	return t
}

//若在unstable和storage的index范围内，发返回term,范围之外返回0
func (l *raftLog) term(index uint64) (uint64, error) {
	//term的正确范围在[dummyindex,last index]
	dummyIndex := l.firstIndex() - 1
	if index < dummyIndex || index > l.lastIndex() {
		//TODO
		//这应该回复一个错误信息吧
		return 0, nil
	}
	if t, ok := l.unstable.maybeTerm(index); ok {
		return t, nil
	}
	t, err := l.storage.Term(index)
	if err == nil {
		return t, nil
	}
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, nil
	}
	panic(err)
}
func (l *raftLog) entries(i, maxSize uint64) ([]pb.Entry, error) {
	if i > l.lastIndex() {
		return nil, nil
	}
	return l.slice(i, l.lastIndex()+1, maxSize)
}

//返回在log中所有的entry
func (l *raftLog) allEntries() []pb.Entry {
	ents, err := l.entries(l.firstIndex(), noLimit)
	if err == nil {
		return ents
	}
	if err == ErrCompacted {
		return l.allEntries()
	}
	panic(err)
}

//判断给定的(index,term)是否比log中所有内容新。
//term 大的新
//term 同，index大的新
func (l *raftLog) isUpToDate(lasti, term uint64) bool {
	return term > l.lastTerm() || (term == l.lastTerm() && lasti >= l.lastIndex())
}

//若在unstable和storage的index范围内，两个term比较。不再范围内term与0进行比较
func (l *raftLog) matchTerm(i, term uint64) bool {
	t, err := l.term(i)
	if err != nil {
		return false
	}
	return t == term
}

//将commited更新到指定index,需要满足相应index,term与log中内容相等
func (l *raftLog) maybeCommit(maxIndex, term uint64) bool {
	if maxIndex > l.committed && l.zeroTermOnErrCompacted(l.term(maxIndex)) == term {
		l.commitTo(maxIndex)
		return true
	}
	return false
}

//将unstable中snap替换，清除unstable中的entries,将committed置为snap.index
//TODO
// applied 不变？？？
func (l *raftLog) restore(s pb.Snapshot) {
	l.logger.Infof("log [%s] starts to restore snapshot [index:%d,term:%d]", l, s.Metadata.Index, s.Metadata.Term)
	l.committed = s.Metadata.Index
	l.unstable.restore(s)
}

//返回log[lo,hi)之间的数据
func (l *raftLog) slice(lo, hi, maxSize uint64) ([]pb.Entry, error) {

	err := l.mustCheckOutOfBounds(lo, hi)
	if err != nil {
		return nil, err
	}
	if lo == hi {
		return nil, nil
	}

	var ents []pb.Entry
	if lo < l.unstable.offset {
		storedEnts, err := l.storage.Entries(lo, min(hi, l.unstable.offset), maxSize)
		if err == ErrCompacted {
			return nil, err
		} else if err == ErrUnavailable {
			l.logger.Panicf("entries[%d,%d) is unavilable from storage", lo, min(hi, l.unstable.offset))
		} else if err != nil {
			panic(err)
		}
		//store中取出的数据满足条件不需要再到unstable中取数据了
		if uint64(len(storedEnts)) < min(hi, l.unstable.offset)-lo {
			return storedEnts, nil
		}
		ents = storedEnts
	}
	if hi > l.unstable.offset {
		unstable := l.unstable.slice(max(lo, l.unstable.offset), hi)
		if len(ents) > 0 {
			//	ents = append([]pb.Entry{},ents...)
			ents = append(ents, unstable...)
		} else {
			ents = unstable
		}
	}
	return limitSize(ents, maxSize), nil
}

// l.firstIndex <= lo <= hi <= l.firstIndex + len(l.entries)
func (l *raftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		l.logger.Panicf("invalid slice %d > %d", lo, hi)
	}
	fi := l.firstIndex()
	if lo < fi {
		return ErrCompacted
	}
	if hi > l.lastIndex()+1 {
		l.logger.Panicf("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, l.lastIndex())
	}
	return nil
}

func (l *raftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted {
		return 0
	}
	l.logger.Panicf("unexpected error (%v) ", err)
	return 0
}
