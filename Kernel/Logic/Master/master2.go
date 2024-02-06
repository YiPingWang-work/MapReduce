package Master

import (
	"MapReduce_v0.1/Kernel/Message"
	"fmt"
	"log"
	"time"
)

/*
初始化master节点
*/

func (m *Master) Init(myid int, slid []int, maxRetry, maxWaitRound, networkDelay int,
	fromBottomChan <-chan Message.Message, toBottomChan chan<- Message.Message) {
	m.id = myid
	m.slaves = []*Slave{}
	m.idleSlaves = []int{}
	for _, v := range slid {
		m.slaves = append(m.slaves, &Slave{
			id:    v,
			state: slave_idle,
		})
		m.idleSlaves = append(m.idleSlaves, v)
	}
	m.maxRetry, m.maxWaitRound = maxRetry, maxWaitRound
	m.deadSlavesNum, m.blockSlavesNum = 0, 0
	m.works = map[int]*Work{}
	m.wid, m.gloid = 0, 0
	m.busy = map[uint64]Bind{}
	m.fromBottomChan, m.toBottomChan = fromBottomChan, toBottomChan
	m.timerEventChan = make(chan GT, 100000)
	m.timerEventX = map[uint64]TimerEvent{}
	m.networkDelay = time.Duration(networkDelay) * time.Millisecond
}

/*
master循环监听，获取消息了类型，传给不同的函数，定时器到期后通知处理超时函数。
*/

func (m *Master) Run() {
	for {
		select {
		case msg, opened := <-m.fromBottomChan:
			if !opened {
				log.Println("Master: msg chan closed")
				log.Println(m.ToString())
				return
			}
			if msg.To != m.id {
				continue
			}
			if msg.Type == Message.ClientReply {
				m.processReplyFromClient(msg.Wid)
			} else if msg.Type == Message.Map {
				m.processMapFinish(msg.Gloid, msg.From, msg.DataPath[0])
			} else if msg.Type == Message.Reduce {
				m.processReduceFinish(msg.Gloid, msg.From, msg.DataPath[0])
			} else if msg.Type == Message.SlaveReply {
				m.processReplyFromSlave(msg.Gloid)
			} else if msg.Type == Message.NewWork {
				m.newWork(msg.Exec, msg.Exec2, msg.DataPath, msg.HashCode,
					msg.From, int(msg.Gloid), msg.Wid)
			}
		case gt, opened := <-m.timerEventChan:
			if !opened {
				log.Println("Master: timer event chan closed")
				return
			}
			m.processTimeout(gt.gloid, gt.eid)
		}
	}
}

/*
处理Map结束任务，如果当前是一个过期的任务，那么查看是否该节点标记为死亡，如果是，则复活节点为idle。
如果是正在进行的任务，完成任务，删除任务bind，该工作添加一个map结果同时通知所有的处理这个工作reduce的slave可以增加处理一个map结果。
如果所有的任务都已经结束，标记该任务为all map finished，之后为处理这个工作的所有reduce的slave添加定时器。
*/

func (m *Master) processMapFinish(gloid uint64, sid int, dataPath string) {
	bind, has := m.busy[gloid]
	if !has {
		if m.slaves[sid].state == slave_dead {
			log.Printf("Master: slave %d revive\n", sid)
			m.deadSlavesNum--
			m.slaves[sid].state = slave_idle
			m.idleSlaves = append(m.idleSlaves, sid)
			m.schedule()
		}
		return
	}
	if bind.sid != sid {
		panic("bind error")
	}
	log.Printf("Master: map finished, bind[%d]: {%s}\n", gloid, bind.ToString())
	delete(m.busy, gloid)
	delete(m.timerEventX, gloid)
	work := m.works[bind.wid]
	work.tasks[bind.tid].state = task_finished
	work.mapResult = append(work.mapResult, dataPath)
	m.slaves[sid].state = slave_idle
	m.idleSlaves = append(m.idleSlaves, sid)
	for v, _ := range work.doReduceSlaves {
		m.toBottomChan <- Message.Message{
			From:      m.id,
			To:        v,
			NeedReply: false,
			Type:      Message.Reduce,
			Gloid:     m.slaves[v].gloid,
			Wid:       work.mapNum,
			DataPath:  work.mapResult,
			Exec:      work.reduceExec,
			HashCode:  work.tasks[m.busy[m.slaves[v].gloid].tid].hashCode,
		}
	}
	if len(work.mapResult) == work.mapNum {
		log.Printf("Master: all map finished, wid: %d\n", work.id)
		work.state = work_all_map_finished
		m.blockSlavesNum -= len(work.doReduceSlaves)
		for v, _ := range work.doReduceSlaves {
			e := TimerEvent{id: 0, gloid: m.slaves[v].gloid, waitRound: m.maxWaitRound,
				needReply: false, timeout: work.reduceTimeout}
			m.addTimerEvent(e)
		}
		work.doReduceSlaves = map[int]bool{}
	}
	m.schedule()
}

/*
处理reduce结束任务，如果当前是一个过期的任务，那么查看是否该节点标记为死亡，如果是，则复活节点为idle。
如果是正在进行的任务，完成任务，删除bind，该工作添加一个reduce结果。
如果所有的reduce任务都已经完成，则同时client工作已完成。
*/

func (m *Master) processReduceFinish(gloid uint64, sid int, dataPath string) {
	bind, has := m.busy[gloid]
	if !has {
		if m.slaves[sid].state == slave_dead {
			log.Printf("Master: slave %d revive\n", sid)
			m.deadSlavesNum--
			m.slaves[sid].state = slave_idle
			m.idleSlaves = append(m.idleSlaves, sid)
			m.schedule()
		}
		return
	}
	if bind.sid != sid {
		panic("bind error")
	}
	log.Printf("Master: reduce finished, bind[%d]: {%s}\n", gloid, bind.ToString())
	delete(m.busy, gloid)
	delete(m.timerEventX, gloid)
	work := m.works[bind.wid]
	work.tasks[bind.tid].state = task_finished
	work.reduceResult = append(work.reduceResult, dataPath)
	m.slaves[sid].state = slave_idle
	m.idleSlaves = append(m.idleSlaves, sid)
	if len(work.reduceResult) == work.reduceNum {
		work.state = work_finished
		log.Printf("Master: work %d finished, result is %v\n", work.id, work.reduceResult)
		m.toBottomChan <- Message.Message{
			From:      m.id,
			To:        work.client,
			NeedReply: true,
			Wid:       work.id,
			Type:      Message.ClientReply,
			DataPath:  work.reduceResult,
		}
	}
	m.schedule()
}

/*
定时器超时函数，每个bind都有唯一一个定时器，有两种定时事件，执行超时和网络超时，如果发现一个任务执行超时，则开始网络超时定时事件，同时
任务可执行数-1，如果网络超时重试几次后依然失败，则master认为slave下线，如果网络恢复则重新开始任务计时，网络重试次数恢复，如果多次
执行超时则master也认为这个任务主观下线。过程中：
一旦收到slave的回复slaveReply则取消网络定时器，开启执行定时器，
一旦收到slave的完成标识则删除所有定时器，该任务成功完成。
如果网络重试次数耗尽或者执行次数耗尽，该slave从working状态转移到dead状态。
*/

func (m *Master) processTimeout(gloid, timerId uint64) {
	e, has := m.timerEventX[gloid]
	if !has || e.id != timerId { // 定时器已经被删除
		return
	}
	delete(m.timerEventX, gloid)
	bind, has := m.busy[e.gloid]
	if !has {
		panic("wrong timer event with no executing event")
	}
	if e.retry == 0 && e.needReply || e.waitRound == 0 { // 丧失了执行能力，这个任务被作废
		log.Printf("Master: timeout, task failed, bind[%d]: {%v}\n", gloid, bind.ToString())
		m.deadSlavesNum++
		m.works[bind.wid].tasks[bind.tid].state = task_unexecuted
		m.slaves[bind.sid].state = slave_dead
		delete(m.busy, gloid)
		m.schedule()
	} else {
		if e.needReply {
			e.retry--
		} else {
			e.retry = m.maxRetry
			e.waitRound--
			e.needReply = true
		}
		log.Printf("Master: timout(1), slave need reply, bind[%d]: {%s}\n", gloid, bind.ToString())
		e.timeout = m.networkDelay
		e.id++
		log.Printf("Master: timout(2), update event: {%s}\n", e.ToString())
		m.addTimerEvent(e)
		work := m.works[bind.wid]
		task := work.tasks[bind.tid]
		msg := Message.Message{
			From:      m.id,
			To:        bind.sid,
			NeedReply: true,
			Gloid:     gloid,
		}
		if task.kind == task_map {
			msg.Type = Message.Map
			msg.DataPath = []string{task.dataPath}
			msg.Exec = work.mapExec
		} else {
			msg.Type = Message.Reduce
			msg.Wid = work.mapNum
			msg.Exec = work.reduceExec
			msg.DataPath = work.mapResult
			msg.HashCode = task.hashCode
		}
		m.toBottomChan <- msg
	}
}

/*
处理回复函数，如果slave对某个任务作出了回复，那么这个任务应该属于定时状态，如果这个任务已经消失了，那么说明该定时任务结束，master不予理睬。
否则查看当前定时函数是否处于需要回复状态，如果不是，说明已经回复过了，这依然是过期消息，不予处理。
否则处理，更新定时器为执行定时器。
*/

func (m *Master) processReplyFromSlave(gloid uint64) {
	e, has := m.timerEventX[gloid]
	if !has {
		return
	}
	if e.needReply {
		delete(m.timerEventX, gloid)
		bind, has := m.busy[gloid]
		if !has {
			panic("wrong timer event with no executing event")
		}
		log.Printf("Master: slave reply(1), bind[%d]: {%s}, event: {%s}\n", gloid, bind.ToString(), e.ToString())
		e.needReply = false
		e.retry = m.maxRetry
		if m.works[bind.wid].tasks[bind.tid].kind == task_map {
			e.timeout = m.works[bind.wid].mapTimeout
		} else {
			e.timeout = m.works[bind.wid].reduceTimeout
		}
		e.id++
		log.Printf("Master: slave reply(2), update timer event: {%s}\n", e.ToString())
		m.addTimerEvent(e)
	}
}

/*
收到客户端的回复，某个任务已经结束，此时master需要删除和这个任务有关的所有数据。
*/

func (m *Master) processReplyFromClient(wid int) {
	if _, has := m.works[wid]; has {
		// 要求数据库删除所有task保存数据的位置
		delete(m.works, wid)
	}
}

/*
客户端请求集群一个新任务，要求给出处理的文件地址，map和reduce函数和hash数量，master根据此划分map任务和reduce任务。
*/

func (m *Master) newWork(mapExecPath, reduceExecPath string,
	data []string, hashCodeNum, from, mt, rt int) {
	m.wid++
	work := Work{
		id:             m.wid,
		tasks:          []*Task{},
		state:          work_begin,
		mapResult:      []string{},
		reduceResult:   []string{},
		doReduceSlaves: map[int]bool{},
		mapNum:         len(data),
		reduceNum:      hashCodeNum,
		mapTimeout:     time.Duration(mt) * time.Millisecond,
		reduceTimeout:  time.Duration(rt) * time.Millisecond,
		client:         from,
		mapExec:        mapExecPath,
		reduceExec:     reduceExecPath,
	}
	for i, v := range data {
		work.tasks = append(work.tasks, &Task{
			id:       i,
			kind:     task_map,
			state:    task_unexecuted,
			dataPath: v,
		})
	}
	for i := 0; i < hashCodeNum; i++ {
		work.tasks = append(work.tasks, &Task{
			id:       i + len(data),
			kind:     task_reduce,
			state:    task_unexecuted,
			hashCode: i,
		})
	}
	m.works[m.wid] = &work
	m.schedule()
}

/*
调度函数，一旦出现idle的slave或者有可能使系统陷入死锁的时候调用此函数。
首先判断是否发生阻塞（所有的存活的slave都在阻塞等待reduce事件），如果有则释放一个绑定关系，将这个slave标记为idle。
之后随机选择一个work，按照先map后reduce取出一个任务分配给一个idle的slave。
*/

func (m *Master) schedule() {
	if m.blockSlavesNum+m.deadSlavesNum == len(m.slaves) {
		log.Println("Master: all alive slaves are block on reduce!")
		if len(m.busy) == 0 {
			log.Println("Master: Oops, your slaves maybe all dead")
			return
		}
		for gloid, bind := range m.busy {
			log.Printf("Master: release a block, bind[%d]: {%s}\n", gloid, bind.ToString())
			m.blockSlavesNum--
			m.works[bind.wid].tasks[bind.tid].state = task_unexecuted
			if _, has := m.works[bind.wid].doReduceSlaves[bind.sid]; !has {
				panic("illegal release slave")
			} else {
				delete(m.works[bind.wid].doReduceSlaves, bind.sid)
			}
			m.slaves[bind.sid].state = slave_idle
			m.idleSlaves = append(m.idleSlaves, bind.sid)
			delete(m.busy, gloid)
			break
		}
	}
	if len(m.idleSlaves) == 0 {
		return
	}
	for _, work := range m.works {
		if work.state == work_finished {
			continue
		}
		for _, task := range work.tasks {
			if task.state == task_unexecuted && len(m.idleSlaves) > 0 {
				if task.kind == task_reduce && work.state == work_begin &&
					m.blockSlavesNum+m.deadSlavesNum+1 == len(m.slaves) {
					continue
				}
				m.gloid++
				sid := m.idleSlaves[0]
				m.idleSlaves = m.idleSlaves[1:]
				m.busy[m.gloid] = Bind{wid: work.id, tid: task.id, sid: sid}
				task.state = task_executing
				m.slaves[sid].state, m.slaves[sid].gloid = slave_working, m.gloid
				msg := Message.Message{
					From:      m.id,
					To:        sid,
					NeedReply: false,
					Gloid:     m.gloid,
				}
				if task.kind == task_map {
					e := TimerEvent{id: 0, gloid: m.gloid, waitRound: m.maxWaitRound,
						needReply: false, timeout: work.mapTimeout}
					m.addTimerEvent(e)
					msg.Type = Message.Map
					msg.DataPath = []string{task.dataPath}
					msg.Exec = work.mapExec
					log.Printf("Master: a new map task send to slave, bind[%d]: {%s}\n", m.gloid, m.busy[m.gloid].ToString())

				} else {
					if work.state == work_begin {
						m.blockSlavesNum++
						work.doReduceSlaves[sid] = true
					} else if work.state == work_all_map_finished {
						e := TimerEvent{id: 0, gloid: m.gloid, waitRound: m.maxWaitRound,
							needReply: false, timeout: work.reduceTimeout}
						m.addTimerEvent(e)
					}
					msg.Type = Message.Reduce
					msg.DataPath = work.mapResult
					msg.Exec = work.reduceExec
					msg.HashCode = task.hashCode
					msg.Wid = work.mapNum
					log.Printf("Master: a new reduce task send to slave, bind[%d]: {%s}\n", m.gloid, m.busy[m.gloid].ToString())

				}
				m.toBottomChan <- msg
			}
			if len(m.idleSlaves) == 0 {
				return
			}
		}
	}
}

/*
新建定时事件。结合timerEventChan和timerEventX进行定时器是否有效判断。
*/

func (m *Master) addTimerEvent(e TimerEvent) {
	m.timerEventX[e.gloid] = e
	go func() {
		time.Sleep(e.timeout)
		m.timerEventChan <- GT{e.gloid, e.id}
	}()
}

func (m *Master) ToString() string {
	res := fmt.Sprintf("=================\nmaster id: %d, gloid: %d, wid: %d\n", m.id, m.gloid, m.wid)
	res += fmt.Sprintf("slaves list:\n{\n")
	for _, v := range m.slaves {
		if v.state == slave_dead {
			res += fmt.Sprintf("  slave %d, state: dead\n", v.id)
		} else if v.state == slave_idle {
			res += fmt.Sprintf("  slave %d, state: idle\n", v.id)
		} else {
			res += fmt.Sprintf("  slave %d, state: working on %d\n", v.id, v.gloid)
		}
	}
	res += fmt.Sprintf("}\nidle slave list: %v\n", m.idleSlaves)
	res += fmt.Sprintf("num of deadSlaves: %d, num of blockSlaves %d\n", m.deadSlavesNum, m.blockSlavesNum)
	res += fmt.Sprintf("works list:\n{\n")
	for _, v := range m.works {
		var state string
		if v.state == work_begin {
			state = "begin"
		} else if v.state == work_all_map_finished {
			state = "all map finished"
		} else {
			state = "finished"
		}
		res += fmt.Sprintf("  -----------------------wid: %d-----------------------\n"+
			"  state: %s, mapNum: %d, reduceNum: %d, client: %d\n",
			v.id, state, v.mapNum, v.reduceNum, v.client)
		res += fmt.Sprintf("  map exec func: %s, reduce exec func: %s\n", v.mapExec, v.reduceExec)
		res += fmt.Sprintf("  tasks: \n  {\n")
		for _, v2 := range v.tasks {
			var state2 string
			if v2.state == task_unexecuted {
				state2 = "unexecuted"
			} else if v2.state == task_executing {
				state2 = "executing"
			} else {
				state2 = "finished"
			}
			if v2.kind == task_map {
				res += fmt.Sprintf("    tid: %d, kind: map, state: %s, datapath %s\n", v2.id, state2, v2.dataPath)
			} else {
				res += fmt.Sprintf("    tid: %d, kind: reduce, state: %s, hashcode: %d\n", v2.id, state2, v2.hashCode)
			}
		}
		res += fmt.Sprintf("  }\n  mapResult: %v\n", v.mapResult)
		res += fmt.Sprintf("  reduceResult: %v\n", v.reduceResult)
		res += fmt.Sprintf("  slave who block on reduce: %v\n", v.doReduceSlaves)
	}
	res += fmt.Sprintf("}\nrunninng tasks:\n{\n")
	for k, v := range m.busy {
		res += fmt.Sprintf("  gloid: %d, %s\n", k, v.ToString())
	}
	res += fmt.Sprintf("}\ntimer:\n{\n")
	for _, v := range m.timerEventX {
		res += fmt.Sprintf("  %s\n", v.ToString())
	}
	res += fmt.Sprintf("}\n=================")
	return res
}

func (b Bind) ToString() string {
	return fmt.Sprintf("wid: %d, tid: %d, sid: %d", b.wid, b.tid, b.sid)
}

func (e TimerEvent) ToString() string {
	return fmt.Sprintf("eid: %d, gloid: %d, waitRound: %d, retry: %d, needReply: %v, timeout: %v",
		e.id, e.gloid, e.waitRound, e.retry, e.needReply, e.timeout)
}
