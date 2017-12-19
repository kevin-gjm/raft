package raft

import (
	"context"
	"errors"

	pb "github.com/coreos/etcd/raft/raftpb"
)

//拷贝过来的什么意思暂时不知道，之后誊写的时候再做处理
type SoftState struct {
	Lead      uint64 // must use atomic operations to access; keep 64-bit aligned.
	RaftState StateType
}