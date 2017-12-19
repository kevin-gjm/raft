### 项目重写誊抄记录

1. 第一步首先找的文件之间没有关联的文件进行誊抄分析。 首先找到的就是logger.go文件。实现Logger接口，封装原生的log，或io/utillog
2. 第二步查看消息类型，了解他们之间的交互方式与结构。誊抄raftpb先的proto文件。查看其格式、类型。然后结合doc.go中对MessageType的注释，了解每一个消息的作用。之后了解到其他大多数的文件都需要引用这一个文件
3. 然后誊写storeage.go文件，原因是独立性较高。实现Storage接口。并实例化一个MemoryStorage。是raft中存储原生log的。由于用到util.go中方法，誊写util.go.
    1. storage中entry的[0]条数据用来记录之前快照中最后的index和term.compact函数(压缩)会将压缩ID之前的所有数据丢弃，保证压缩ID < applied 是应用程序的责任
    2. 将storeage_test.go文件拷贝进去。进行测试 
4. 之后应该是log.go中使用log_unstable.go .不严谨的storage数据存储部分