# MapReduce基础实现



### Master逻辑：

#### Master存储要点

```go
type Master struct {
    slaves []Slave // 所有的奴隶
    idleSlaves []int // 所有空闲奴隶的编号
    works map[int]*Work // 所有的作业
    msgChan chan <-chan Message // 消息 
    timerChan chan TimerEvent // 定时器
    timerX map[int]bool // 计时任务是否有效，里面的int指的是Gloid
}
type TimerEvent struct {
    flag bool // true 普通 / false 网络
    x int // 重试的机会，默认3次
    msg Message // 什么命令触发的这个定时任务
}

type Slave struct {
    sid int // 自己的ID
    wid int // 自己正在执行的作业ID，空闲或死亡为-1 
    tid int // 自己正在执行的作业的任务ID，空闲或死亡为-1
    state int // 该奴隶所处的执行阶段
}

type Work struct {
    wid int // 作业ID
    tasks []*Task // 作业有哪些任务
    flag int // 0 start 1 allMapFinished 2 Finished
    finishedMap []int // 已经完成Map任务的节点
    finishedReduce []int //已经完成Reduce任务的节点
    doReduce []int // 正在阻塞处理reduce任务的节点
    mapNum int // 一共的map数量
    reduceNum int // 一共的reduce数量，len(tasks) - mapNum
}

type Task struct {
    tid int // 我的ID
    wid int // 我属于哪个工作
    sid int // 哪个奴隶在执行
    state int // 该作业所处的执行阶段
    gloid int // 任务唯一ID，一旦更新就自增
    Type bool // true Map / false Reduce
    execFunc string // 执行函数文件地址，本地有不从数据库拉取
    hashFunc string // Hash生成函数文件地址（Map阶段使用），本地有不从数据库拉取
    hashCode int // 哈希码（Reduce阶段使用）
    
}
```

每当master发布一个任务，其任务的gloid都会全局自增1，slave只会处理比自己gloid高的任务（平级不处理，如果是平级同时需要回复，同时此时该任务正在处理或已经处理完，则发送一条OK消息），并且是打断式处理

#### porcessMapFinished 处理map结束函数（参数：wid, tid, gloid, sid）

1.通过wid, tid定位到某个任务，取出这个任务的gloid。

2.如果gloid对应，同时这个任务还处于running，则说明这是一个正确执行完成的节点，

​	更新作业状态：添加finishedMap <- sid，

​	更新任务状态：state=finished

​	更新节点状态：state=Idle, wid=-1,tid=-1，

​	删除定时任务，删除相关的Gloid，

3.对2，否则，如果slaves[sid].state=Dead，更新为slaves[sid].state=Idle

4.接2，向所有doReduce的slave节点发送一则更新消息（finishedmap增加了）

5.接2，如果此时finishedMap的长度等于mapNum，则说明该作业的所有map执行完成，将该作业状态更新为1（allMapFinished），同时给所有的doReduce节点一个定时信息，信息上添加4中的消息

6.启用schedule函数，因为刚刚释放一个Idle的节点，需要调度。



#### porcessReduceFinished 处理reduce结束函数（参数：wid, tid, gloid, sid）

1.通过wid, tid定位到某个任务，取出这个任务的gloid。

2.如果gloid对应，同时这个任务还处于running，则说明这是一个正确执行完成的节点，

​	更新作业状态：添加finishedReduce <- sid，

​	更新任务状态：state=finished

​	更新节点状态：state=Idle, wid=-1,tid=-1

​	删除定时任务，删除相关的Gloid，

3.对2，否则，如果slaves[sid].state=Dead，更新为slaves[sid].state=Idle

4.接2，如果此时finishedReduce的长度等于reduceNum，则说明所有的reduce执行完成，将该作业状态更新为2（Finished），同时通知client消息，告诉它所有的结果地址（finishedReduce）。

6.启用schedule函数，因为刚刚释放一个Idle的节点，需要调度。



#### processTimeout 处理定时器到期（参数：TimerEvent）

0.查看这个定时任务是否有效，无效立即返回

2.如果有效：

​	2.1.如果它是一个网络超时任务或者一般超时任务但是可用次数x=0，则将slave与该任务解绑，设置slave的状态为Dead（死亡），这是任务的状态为Untreated（未执行），master认为这个任务主观下线，同时调用schedule函数，因为可能出现死锁。

​	2.2.如果它是一个一般超时任务可用次数x>0（Map任务或者Reduce结束任务），则重新发送一遍该任务（msg，Gloid不变），同时要求slave节点回复（master怀疑是网络波动）设置新的定时任务它的任务Gloid和原来保持不变，但是flag变为false（网络）

3.删除处理过的任务。



#### processReply 处理客户端回复命令，该命令是验活后才会收到（参数：Message）

这个命令发送的条件是slave收到了一个比自己大的Gloid，说明是master的网络波动

master校验gloid在定时任务中是否还有这一项：（没有说明这是一个过期的消息，已经不具备意义，直接返回）

1.删除定时任务（删除timerX中gloid那项）

2.更新这个任务的定时器（重新设置一个普通的定时任务，任务gloid保持不变，flag设置为true，x自减）



#### schedule 处理调度

如果当前所有的主观存活节点都处于Reduce状态同时所有的作业中都都处于0阶段，则随机将某个执行reduce节点解绑。

之后遍历所有任务，如果存在Untreated的，优先处理老的Map任务，但需要时刻保证不能陷入上述局面，在分发reduce任务的时候如果该reduce任务的作业已经处于allMapFinished状态，则增加一个定时器进程。





#### 其它

基础上必须保证所有slave节点都在回复master前将所有的数据存储在一个中间数据库中，且要求这个数据库是高可用的



### slave逻辑

slave如果收到个消息，会比较自己号，如果是一个较大的信息，则会中断当前任务处理，如果是一个等于自己的消息同时自己的任务已经完成，则返回一个任务完成后的响应，如果等于自己的消息或大于自己的消息此时任务未完成（不是正在进行中，是压根没见过这个命令，说明是master出现网络问题没有把消息发出来或者消息迟到），那么发送一个存活回复。

salve必须将自己的中间结果和结果成功保存在数据库才可以给master发送完成信息。

slave将会保存最新的处理完成信息，map处理完成或者reduce全部处理完成