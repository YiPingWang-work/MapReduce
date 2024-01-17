package Logic

import (
	"MapReduce_v0.1/Kernel/Message"
	"time"
)

const (
	Map int = iota
	Reduce
	Idle
	Dead
)

type Master struct {
	slaves         []slave
	idleSlaves     []int
	tasks          map[int][]task
	mapTasks       map[int]map[int]bool // 还没开始的map任务
	reduceTasks    map[int]map[int]bool // 还没开始的reduce任务
	runningTasks   map[int]map[int]int  // 还在运行的任务
	fromBottomChan <-chan Message.Message
	toBottomChan   chan<- Message.Message
	timerChan      chan int
}

/*
存储所有从节点信息，这里可以不用维护IP，我觉得可以在网络层维护
这里主要存储这个节点的定时信息，正在执行的任务信息，以及这个节点的状态
*/

type slave struct {
	id     int              // 从节点的Id
	state  int              // slave所处的状态
	taskId int              // 这个节点所执行的节点的Id
	timer  <-chan time.Time // 计时信息
}

/*
任务封装，里面应该是一个string -> kv的函数或者 kv -> kv的函数
同时还应有一个输入数据
同时还有一个Hash函数，负责计算标识产出的key应该放在内存的位置
*/

type task struct {
	pid      int           // 集合任务ID
	id       int           // 任务ID
	taskType int           // 任务类型
	slaveId  int           // 执行的slave节点编号
	data     string        // 任务所需数据文件存放位置（map任务）
	hashCode int           // 任务所需要的摄取的hash码（reduce任务）
	timeout  time.Duration // 到期时间
}

func (m *Master) init() {

}

func (m *Master) processFinishedMap(tpid int, tid int, sid int) { // 处理完成的Map任务
	// 解绑slave节点
	m.slaves[sid].state, m.slaves[sid].taskId = Idle, -1
	m.idleSlaves = append(m.idleSlaves, sid)
	if ts, has := m.runningTasks[tpid]; !has { // 如果该项目已经完成，放弃返回
		return
	} else if s, has := ts[tid]; !has { // 如果该任务已经完成，放弃返回
		return
	} else if s != sid { // 如果该任务的slave不再是这个slave，放弃返回
		return
	}
	// 否则这个任务已经完成
	delete(m.runningTasks[tpid], tid)
	if len(m.mapTasks[tpid]) == 0 { // 所有的Map任务都已经完成了
		for v, _ := range m.reduceTasks[tpid] {
			m.publishReduce(m.tasks[tpid][v])
		}
	} else {
		for v, _ := range m.mapTasks[tpid] {
			m.publishMap(m.tasks[tpid][v])
			break
		}
	}
}

func (m *Master) publishReduce(t task) {

}

func (m *Master) publishMap(t task) {

}

func (m *Master) processFinishedReduce(tpid int, tid int, sid int) { // 处理完成的Reduce任务
	m.slaves[sid].state, m.slaves[sid].taskId = Idle, -1
	m.idleSlaves = append(m.idleSlaves, sid)
	if ts, has := m.runningTasks[tpid]; !has { // 如果该项目已经完成，放弃返回
		return
	} else if s, has := ts[tid]; !has { // 如果该任务已经完成，放弃返回
		return
	} else if s != sid { // 如果该任务的slave不再是这个slave，放弃返回
		return
	}
	// 否则这个任务已经完成
	delete(m.runningTasks[tpid], tid)
	if len(m.runningTasks[tpid]) == 0 { // 所有的Reduce任务都已经完成了

	}
}

func (m *Master) processTimeout(tid int) { // 处理超时的任务

}

/*
新建任务，包括希望划分的Map任务数量、洗牌阶段的hash函数，以及map函数，reduce函数。
*/

func (m *Master) processInitTask(mapN int, hashFunc string, mapFunc string, reduceFunc string) {

}
