package Logic

import "time"

type Master struct {
	slaves        []slave
	idleSlaves    []int
	tasks         []task
	finishedTasks []int
}

type funcBytes struct {
	content []byte // 可执行文件
}

/*
存储所有从节点信息，这里可以不用维护IP，我觉得可以在网络层维护
这里主要存储这个节点的定时信息，正在执行的任务信息，以及这个节点的状态
*/

type slave struct {
	id        int              // 从节点的Id
	slaveType int              // 节点的类型
	taskId    int              // 这个节点所执行的节点的Id
	timer     <-chan time.Time // 计时信息
}

/*
任务封装，里面应该是一个string -> kv的函数或者 kv -> kv的函数
同时还应有一个输入数据
同时还有一个Hash函数，负责计算标识产出的key应该放在内存的位置
*/

type task struct {
	id       int           // 任务ID
	taskType int           // 任务类型
	slaveId  int           // 执行的slave节点编号
	data     []byte        // 任务所需数据
	hashFunc *funcBytes    // 存储的是执行函数标识
	taskFunc *funcBytes    // 存储的是执行函数标识
	timeout  time.Duration // 到期时间
}

/*
实现一个函数，对于一个任务实体，需要找到一个slave节点进行分配，并记录这则信息
*/

func (m *Master) schedule() {

}

/*
需要实现一个节点注册与发现的函数，入股收到节点，会在这里实现注册功能
*/

func (m *Master) register() {

}

/*
重新设计，包括重新构建MapReduce任务和对任务步骤进行拆分
*/

func (m *Master) redesign() {

}

/*
预同步机制，所有的Reduce节点需要得知Map节点的位置，方便进行拉取数据，每当一个Map任务完成后，master将这个Map通知所有进行Reduce任务的节点
第一版本设计通知给所有的节点，因为每个节点都有可能承担Reduce任务
*/

func (m *Master) preReduce() {

}

/*
实现一个函数，监视所有slave节点的运行状态，包括该任务是否结束，是否超时
*/

func (m *Master) run() {

}
