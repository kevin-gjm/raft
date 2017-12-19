package raft

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
