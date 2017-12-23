### 项目重写誊抄记录

1. 第一步首先找的文件之间没有关联的文件进行誊抄分析。 首先找到的就是logger.go文件。实现Logger接口，封装原生的log，或io/utillog
2. 第二步查看消息类型，了解他们之间的交互方式与结构。誊抄raftpb先的proto文件。查看其格式、类型。然后结合doc.go中对MessageType的注释，了解每一个消息的作用。之后了解到其他大多数的文件都需要引用这一个文件
3. 然后誊写storeage.go文件，原因是独立性较高。实现Storage接口。并实例化一个MemoryStorage。是raft中存储原生log的。由于用到util.go中方法，誊写util.go.
    1. storage中entry的[0]条数据用来记录之前快照中最后的index和term.compact函数(压缩)会将压缩ID之前的所有数据丢弃，保证压缩ID < applied 是应用程序的责任
    2. 将storeage_test.go文件拷贝进去。进行测试 
4. 之后应该是log.go中使用log_unstable.go .不严谨的storage数据存储部分
    log.go 中使用storage与unstable两个存储，管理commited与applied两个index. 添加新的entries 到unstable中(管理其中发生的冲突)。compact会丢弃之前的数据。封装一些unstable和storage中的函数
5. entry的管理誊写完成，之后应该是整个raft流程的誊写。首先誊写node.go节点。raft中最小的单位(貌似需要先写raft节点，因为raft中只是但单纯的内一个步骤，而node则是对这些步骤的封装)
6. Progress.go:在领导者严重跟随着的过程，领导者有所有跟随着的状态，并且根据其状态发送消息给跟随着
    1. 内部含有match 和next 两个index参数。存储有infliaghts：内部含有一个存储每条消息中log entries中最last的index的一个循环使用的buffer
    2. 通过Progress的state状态来判断Progress的操作和下一步的操作
7. 继续写raft.go 和 node.go
