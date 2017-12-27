package raft 

import pb "github.com/coreos/etcd/raft/raftpb"

//提供只读请求的状态
//在获取状态前先读ReadIndex是调用者的责任。
//同样根据RequestCtx识别具体的调用内容
type ReadState struct {
	Index	uint64
	RequestCtx	[]byte
}

type readIndexStatus struct {
	req pb.Message
	index uint64
	//用于收集对端的回复消息，key为对端id: msg.from
	acks map[uint64]struct{}
}

type readOnly struct {
	option ReadOnlyOption
	//msg中第一条log的data，作为key
	pendingReadIndex map[string]*readIndexStatus
	//也是用上面的那个string作为唯一标识的
	readIndexQueue []string
}

func newReadOnly(option ReadOnlyOption) *readOnly {
	return & readOnly {
		option: option,
		pendingReadIndex :make(map[string]*readIndexStatus),
	}
}

//添加一个只读请求到readonly结构中
//index 收到只读请求时，raft状态机的commit index
//m 来自对端node的只读请求
func (ro *readOnly) addRequest(index uint64, m pb.Message) {
	ctx := string(m.Entries[0].Data)
	if _,ok:= ro.pendingReadIndex[ctx];ok {
		return
	}
	ro.pendingReadIndex[ctx] = &readIndexStatus{index:index,req:m,acks:make(map[uint64]struct{})}
	ro.readIndexQueue= append(ro.readIndexQueue,ctx)
}
//用于收集对端的回复消息，并返回收集到的总个数。通过msg中的context进行关联
func (ro *readOnly) recvAck(m pb.Message) int {
	rs,ok := ro.pendingReadIndex[string(m.Context)]
	if !ok {
		return 0
	}
	rs.acks[m.From] = struct{}{}
	return len(rs.acks) + 1
}

//删掉readonly的queue中包含m消息在内之前的内容。并清除相应的pendingReadIndex。返回所有清除的readIndexStatus
func (ro *readOnly) advance(m pb.Message) []*readIndexStatus {
	var (
		i int
		found bool
	)

	ctx := string(m.Context)
	rss := []*readIndexStatus{}

	for _,okctx := range ro.readIndexQueue {
		i++
		rs,ok := ro.pendingReadIndex[okctx]
		if !ok {
			panic("cannot find corresponding read state from pending map")
		}
		rss = append(rss,rs)
		if okctx == ctx {
			found = true
			break
		}
	}
	if found {
		ro.readIndexQueue = ro.readIndexQueue[i:]
		for _,rs := range rss {
			delete(ro.pendingReadIndex,string(rs.req.Entries[0].Data))
		}
		return rss
	}
	return nil
}

func (ro *readOnly) lastPendingRequestCtx() string {
	if len(ro.readIndexQueue) == 0 {
		return ""
	}
	return ro.readIndexQueue[len(ro.readIndexQueue)-1]
}