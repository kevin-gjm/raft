
package raft

import "fmt"

const (
	ProgressStateProbe	ProgressStateType = iota
	ProgressStateReplicate 
	ProgressStateSnapshot
)

type ProgressStateType uint64

var prstmap = [...]string {
	"ProgressStateProbe",
	"ProgressStateReplicate",
	"ProgressStateSnapshot",
}


func (st ProgressStateType) String() string {
	return prstmap[uint64(st)]
}

//在领导者严重跟随着的过程，领导者有所有跟随着的状态，并且根据其状态发送消息给跟随着
type Progress struct {
	Match,Next  uint64

	//详见design.md文档
	//state状态，决定着leader如何与之交互

	//ProgressStateProbe:leader在心跳中最多附加发送一条msg。为了探查跟随着实际的state
	//ProgressStateReplicate:发送next到latest之间的msg,这是两者之间交互的最佳方式
	//ProgressStateSnapshot：不发送任何replication message
	State ProgressStateType

	//在ProgressStateProbe下使用，当true时leader不在给此perr发送replication message
	Paused bool
	
	//在ProgressStateSnapshot下使用
	//若存在一个snapshot,则PendingSnapshot被设置为snap的index。
	//若已经被设置，此过程的复制过程将被暂停。raft将不会重发snap直到pending one reported failed
	PendingSnapshot uint64

	//收到相应对端的任何消息标志着progress is active.
	//将会被设置false在一个election timeout之后
	RecentActive bool


	//一个inflghts消息的滑动窗口
	//每一条inflights包含一条或多条log entries.条数的最大值是raft的MaxSizePerMsg
	//因此inflght有效的限制了inflight消息的数量和每一个Progress使用的brandwidth

	//当满时，不能够在再发送任何信息(接收？？)，当leader发送出去一条信息后，最后一条在没加入到inflghts中。在inflights中index必须是有序的
	//当leader收到一条回复，之前的infilghts将通过调用inflights.freeTo释放
	ins *inflights

	IsLearner bool
}

func (pr *Progress) resetState(state ProgressStateType){
	pr.Paused = false
	pr.PendingSnapshot = 0
	pr.State = state
	pr.ins.reset()
}

func(pr *Progress) becomeProbe(){
	//若之前的状态是snap，progress就会知道对端成功添加了snap。然后probe应该从pendingSnapshot+1
	if pr.State == ProgressStateSnapshot {
		PendingSnapshot := pr.PendingSnapshot
		pr.resetState(ProgressStateProbe)
		pr.Next = max(pr.Match+1,PendingSnapshot+1)
	}else {
		pr.resetState(ProgressStateProbe)
		pr.Next = pr.Match + 1 
	}
}

func (pr *Progress)becomeReplicate() {
	pr.resetState(ProgressStateReplicate)
	pr.Next = pr.Match + 1
}
func (pr *Progress) becomeSnapshot(snapshotindex uint64) {
	pr.resetState(ProgressStateSnapshot)
	pr.PendingSnapshot = snapshotindex
}

//若给定参数来自一个过期的消息，返回false
//其他将内部match和next更新为n和n+1
func (pr *Progress) maybeUpdate(n uint64) bool {
	var updated bool //默认值 false
	if pr.Match < n {
		pr.Match = n
		updated = true
		pr.resume()
	}
	if pr.Next <  n+1 {
		pr.Next = n+1
	}
	return updated
}

func(pr *Progress)optimisticUpdate(n uint64) { pr.Next = n + 1}

//若给定的index是来自一个过期的message返回false
//否则将Progress的next index 更新为 min(rejected,last) 返回true
func (pr *Progress) maybeDecrTo(rejected,last uint64) bool {
	if pr.State == ProgressStateReplicate {
		//rejected 是陈旧的，直接返回false
		if rejected <= pr.Match {
			return false
		}
		//直接将next置为match确认一样的后一个。
		pr.Next = pr.Match + 1
		return true
	}

	//TODO没看懂？？？
	// the rejection must be stale if "rejected" does not match next - 1
	if pr.Next -1 != rejected {
		return false
	}

	if pr.Next = min(rejected,last+1);pr.Next <1 {
		pr.Next = 1
	}
	return true
}

func(pr *Progress) pause() {	pr.Paused  = true	}
func(pr *Progress) resume() {	pr.Paused = false	}

func (pr *Progress) IsPaused() bool {
	switch pr.State {
	case ProgressStateProbe:
		return pr.Paused
	case ProgressStateReplicate:
		return pr.ins.full()
	case ProgressStateSnapshot:
		return true
	default:
		panic("unexpected state")
	}
}

func (pr * Progress) snapshotFailure() { pr.PendingSnapshot = 0 }

func (pr *Progress) needSnapshotAbort() bool {
	return pr.State == ProgressStateSnapshot && pr.Match >= pr.PendingSnapshot 
}

func(pr *Progress) String () string {
	return fmt.Sprintf("next = %d,match = %d,state = %s,waiting = %v,pendingSnapshot = %d",
		pr.Next,pr.Match,prstmap[pr.State],pr.IsPaused(),pr.PendingSnapshot)
}

type inflights struct {
	//buffer中的起始index
	start int

	count int

	size int
	//包含内容是，每条消息中最后的log entry index
	//循环使用的，类似环形缓冲区
	buffer []uint64
}

func newInflights(size int) *inflights {
	return &inflights {
		size:size,
	}
}

func (in *inflights) add(inflights uint64) {
	if in.full() {
		panic("cannot add into a full inflights")
	}
	next := in.start+in.count
	size := in.size
	if next >= size {
		next -= size
	}

	if next >= len(in.buffer) {
		in.growBuf()
	}
	in.buffer[next] = inflights
	in.count++
}

func (in *inflights)growBuf() {
	newSize := len(in.buffer)*2
	if newSize == 0 {
		newSize =1
	}else if newSize > in.size {
		newSize = in.size
	}

	newBuffer := make([]uint64,newSize)
	copy(newBuffer,in.buffer)
	in.buffer = newBuffer
}

//释放inflights小于等于to的flights
func (in* inflights) freeTo(to uint64) {
	if in.count == 0 || to < in.buffer[in.start] {
		return 
	}
	idx := in.start

	//计算出要释放多少个inflight
	var i int
	for i =0;i < in.count ;i++ {
		if to < in.buffer[idx] {
			break
		}

		size := in.size 
		if idx++; idx >= size {
			idx -= size
		}
	}

	//释放i个inflight实际上buffer中的内容不删除，只覆盖，通过start和count配合确定是否有效
	in.count -= i
	in.start = idx
	if in.count == 0 {
		//若全空，没有数据，则将start放在初始位置。方便操作
		in.start = 0
	}
}

func (in *inflights) freeFirstOne() {
	in.freeTo(in.buffer[in.start])
}

func (in *inflights) full() bool {
	return in.count == in.size
}

func (in *inflights) reset(){
	in.count =0 
	in.start =0 
}