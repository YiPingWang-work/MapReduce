package Logic

import (
	"MapReduce_v0.1/Kernel/Message"
	"log"
	"math/rand"
	"time"
)

type Master struct {
	works          map[int]*Work
	slaves         []*Slave
	idleSlaves     []int
	unMapNum       int // 无法从事map任务的节点数量，如果全部都在从事reduce节点，那么需要从某些reduce节点中调走一些从事map任务
	timer          <-chan time.Time
	timerEvents    []timerEvent
	fromBottomChan <-chan Message.Message
	toBottomChan   chan<- Message.Message
}

const (
	Map int = iota
	Reduce
	Idle
	Dead
)

const (
	Untreated int = iota
	Running
	Finished
)

type timerEvent struct {
	timestamp time.Duration
	wid       int
	tid       int
}

type Work struct {
	wid                  int     // 作业号
	tasks                []*Task // 作业中的任务
	mapNum               int     // 还未完成的map任务数量
	reduceNum            int     // 还未完成的reduce任务数量
	finishedMapSlaves    []int   // 处理完map任务的slave节点（这些节点会保留一些数据供后续的Map使用）
	finishedReduceSlaves []int   // 处理完map任务的slave节点（这些节点会保留一些数据供后续的Map使用）
}

type Task struct {
	wid      int           // 作业号
	tid      int           // 任务号
	state    int           // 作业执行的状态
	sid      int           // 正在执行该任务的slave编号
	kind     int           // 任务种类
	execPath string        // 执行文件位置
	dataPath string        // 数据位置（kind = map时有效）
	hashCode int           // 获取哈希码（kind = reduce时有效）
	timeout  time.Duration //超时时间
}

type Slave struct {
	id        int             // slave号
	state     int             // slave状态
	wid       int             // slave执行的作业号
	tid       int             // 该slave执行的任务号
	timestamp int             // 最后一次收到该slave回复的时间戳
	lastMsg   Message.Message // master最后发给该slave的消息
}

func (m *Master) processMapFinished(wid, tid, sid, timestamp int) {
	m.slaves[sid].state = Idle
	m.slaves[sid].wid = -1
	m.slaves[sid].tid = -1
	m.slaves[sid].timestamp = timestamp
	m.idleSlaves = append(m.idleSlaves, sid)
	m.unMapNum--
	work := m.works[wid]
	task := work.tasks[tid]
	if task.kind != Map {
		panic("processMapFinished: it's not a map task")
	}
	if task.state != Running || task.sid != sid {
		return
	}
	task.state = Finished
	work.mapNum--
	work.finishedMapSlaves = append(work.finishedMapSlaves, sid)
	for _, v := range work.tasks {
		if v.kind == Reduce && v.state == Running {
			// 发送一个需要增加处理一个节点数据的信息的消息
		}
	}
	if work.mapNum == 0 {
		// 通知所有正处理该作业Reduce的节点，可以返回，这则消息需要带着所有数据
		// 增加定时任务
	}
	m.schedule()
}

func (m *Master) processReduceFinished(wid, tid, sid, timestamp int) {
	m.slaves[sid].state = Idle
	m.slaves[sid].wid = -1
	m.slaves[sid].tid = -1
	m.slaves[sid].timestamp = timestamp
	m.idleSlaves = append(m.idleSlaves, sid)
	m.unMapNum--
	work := m.works[wid]
	task := work.tasks[tid]
	if task.kind != Reduce {
		panic("processMapFinished: it's not a reduce task")
	}
	if task.state != Running || task.sid != sid {
		return
	}
	task.state = Finished
	work.reduceNum--
	work.finishedReduceSlaves = append(work.finishedReduceSlaves, sid)
	if work.reduceNum == 0 {
		// 该任务结束，信息数据都保存在finishedReduceSlaves这些节点的磁盘文件中
		for _, v := range work.tasks {
			if m.slaves[v.sid].state == Running && m.slaves[v.sid].wid == wid {
				m.slaves[v.sid].state = Dead
			}
		}
		delete(m.works, wid)
	}
	m.schedule()
}

func (m *Master) processTimeout(wid, tid int) {
	work := m.works[wid]
	task := work.tasks[tid]
	sid := task.sid
	m.slaves[sid].state = Dead
	m.slaves[sid].wid = -1
	m.slaves[sid].tid = -1
	task.state = Untreated
	task.sid = -1
	m.unMapNum++
	if len(m.slaves) == m.unMapNum { // 必须剥夺一个reduce节点的执行
		idx := -1
		rand.Seed(time.Now().UnixNano())
		for i := 0; i < 10000; i++ {
			idx = rand.Intn(len(m.slaves))
			if m.slaves[idx].state == Running {
				break
			}
		}
		if idx == -1 {
			log.Println("Master warning: maybe no alive slaves")
		} else {
			m.slaves[idx].state = Idle
			m.slaves[idx].tid = -1
			m.idleSlaves = append(m.idleSlaves, idx)
			m.unMapNum--
		}
	}
	m.schedule()
}

func (m *Master) schedule() int {
	for _, work := range m.works {
		for _, task := range work.tasks {
			if task.state == Untreated {
				if len(m.idleSlaves) > 0 {
					if task.kind == Reduce && m.unMapNum < len(m.slaves)-1 { // 不会让所有的slave节点都处于reduce状态，必须至少存活一个可以map的节点
						m.unMapNum++
					} else if task.kind == Reduce {
						break
					} else if task.kind == Map {
						// 增加定时任务
					}
					sid := m.idleSlaves[0]
					m.idleSlaves = m.idleSlaves[1:]
					task.sid = sid
					task.state = Running
					// 发送一个消息给这个sid他有一个新Task要处理
				} else {
					return 0
				}
			}
		}
	}
	return len(m.idleSlaves)
}

func (m *Master) Run() {
	for {
		select {
		case msg, opened := <-m.fromBottomChan:
			if !opened {
				panic("Master: chan closed")
			}
			if msg.Type == Message.Map {
				m.processMapFinished(msg.Wid, msg.Tid, msg.From, msg.Timestamp)
			} else if msg.Type == Message.Reduce {
				m.processReduceFinished(msg.Wid, msg.Tid, msg.From, msg.Timestamp)
			}
		case <-m.timer:

			m.processTimeout(m.timerEvents[0].wid, m.timerEvents[0].tid)
			m.timerEvents = m.timerEvents[1:]
		}
	}
}
