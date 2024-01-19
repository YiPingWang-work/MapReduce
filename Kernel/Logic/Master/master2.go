package Master

import (
	"MapReduce_v0.1/Kernel/Message"
	"time"
)

func (m *Master) processMapFinish(gloid, sid int, dataPath string) {
	bind, has := m.busy[gloid]
	if !has {
		if m.slaves[sid].state == slave_dead {
			m.deadSlavesNum--
			m.slaves[sid].state = slave_idle
			m.idleSlaves = append(m.idleSlaves, sid)
			m.schedule()
		}
		return
	}
	delete(m.busy, gloid)
	delete(m.timerEventX, gloid)
	work := m.works[bind.wid]
	work.tasks[bind.tid].state, work.tasks[bind.tid].dataPath = task_finished, dataPath
	work.finishedMap = append(work.finishedMap, dataPath)
	m.slaves[sid].state = slave_idle
	m.idleSlaves = append(m.idleSlaves, sid)
	for _, v := range work.doReduceSlaves {
		m.toBottomChan <- Message.Message{
			From:      m.id,
			To:        v,
			NeedReply: false,
			Type:      Message.Reduce,
			Gloid:     m.slaves[v].gloid,
			Wid:       work.mapNum,
			Data:      work.finishedMap,
			ExecFunc:  work.reduceExecPath,
			HashCode:  work.tasks[m.busy[m.slaves[v].gloid].tid].hashCode,
		}
	}
	if len(work.finishedMap) == work.mapNum {
		work.state = work_all_map_finished
		m.blockSlavesNum -= len(work.doReduceSlaves)
		for _, v := range work.doReduceSlaves {
			e := TimerEvent{gloid: m.slaves[v].gloid, x: 3, needReply: false, timeout: work.reduceTimeout}
			m.addTimerEvent(e)
		}
	}
	m.schedule()
}

func (m *Master) processReduceFinish(gloid, sid int, dataPath string) {
	bind, has := m.busy[gloid]
	if !has {
		if m.slaves[sid].state == slave_dead {
			m.deadSlavesNum--
			m.slaves[sid].state = slave_idle
			m.idleSlaves = append(m.idleSlaves, sid)
			m.schedule()
		}
		return
	}
	delete(m.busy, gloid)
	delete(m.timerEventX, gloid)
	work := m.works[bind.wid]
	work.tasks[bind.tid].state, work.tasks[bind.tid].dataPath = task_finished, dataPath
	work.finishedReduce = append(work.finishedReduce, dataPath)
	m.slaves[sid].state = slave_idle
	m.idleSlaves = append(m.idleSlaves, sid)
	if len(work.finishedMap) == work.mapNum {
		work.state = work_finished
		m.toBottomChan <- Message.Message{
			From:      m.id,
			To:        work.client,
			NeedReply: true,
			Wid:       work.id,
			Type:      Message.ClientReply,
			Data:      work.finishedReduce,
		}
	}
	m.schedule()
}

func (m *Master) processTimeout(gloid int) {
	e, has := m.timerEventX[gloid]
	if !has { // 定时器已经被删除
		return
	}
	delete(m.timerEventX, e.gloid)
	bind, has := m.busy[e.gloid]
	if !has {
		panic("wrong timer event with no executing event")
	}
	if e.x == 0 || e.needReply == true { // 丧失了执行能力，这个任务被作废
		m.deadSlavesNum++
		m.works[bind.wid].tasks[bind.tid].state = task_unexecuted
		m.slaves[bind.sid].state = slave_dead
		delete(m.busy, e.gloid)
		m.schedule()
	} else {
		e.needReply = true
		e.timeout = m.networkDelay
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
			msg.Data = []string{task.dataPath}
			msg.ExecFunc = work.mapExecPath
			msg.HashFunc = work.hashPath
		} else {
			msg.Type = Message.Reduce
			msg.Wid = work.mapNum
			msg.ExecFunc = work.reduceExecPath
			msg.Data = work.finishedMap
			msg.HashCode = task.hashCode
		}
		m.toBottomChan <- msg
	}
}

func (m *Master) processReplyFromSlave(gloid int) {
	e, has := m.timerEventX[gloid]
	if !has { // 定时器已经被删除
		return
	}
	if e.needReply {
		delete(m.timerEventX, gloid)
		e.needReply = false
		e.x--
		bind, has := m.busy[gloid]
		if !has {
			panic("wrong timer event with no executing event")
		}
		if m.works[bind.wid].tasks[bind.tid].kind == task_map {
			e.timeout = m.works[bind.wid].mapTimeout
		} else {
			e.timeout = m.works[bind.wid].reduceTimeout
		}
		m.addTimerEvent(e)
	}
}

func (m *Master) addTimerEvent(e TimerEvent) {
	m.timerEventX[e.gloid] = e
	go func() {
		time.Sleep(e.timeout)
		m.timerEventChan <- e.x
	}()
}

func (m *Master) processReplyFromClient(wid int) {
	if _, has := m.works[wid]; has {
		// 要求数据库删除所有task保存数据的位置
		delete(m.works, wid)
	}
}

func (m *Master) newWork(mapExecPath, reduceExecPath, hashPath string, data []string, hashCodeNum int, from int) {
	m.wid++
	work := Work{
		id:             m.wid,
		tasks:          []*Task{},
		state:          work_begin,
		finishedMap:    []string{},
		finishedReduce: []string{},
		doReduceSlaves: []int{},
		mapNum:         len(data),
		reduceNum:      hashCodeNum,
		mapTimeout:     time.Duration(10000) * time.Millisecond,
		reduceTimeout:  time.Duration(10000) * time.Millisecond,
		client:         from,
		mapExecPath:    mapExecPath,
		reduceExecPath: reduceExecPath,
		hashPath:       hashPath,
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
	m.schedule()
}

func (m *Master) schedule() {
	if m.blockSlavesNum+m.deadSlavesNum == len(m.slaves) {
		for gloid, bind := range m.busy {
			m.blockSlavesNum--
			m.works[bind.wid].tasks[bind.tid].state = task_unexecuted
			m.slaves[bind.sid].state = slave_idle
			m.idleSlaves = append(m.idleSlaves, bind.sid)
			delete(m.busy, gloid)
			break
		}
	}
	for _, work := range m.works {
		if work.state == work_finished {
			continue
		}
		for _, task := range work.tasks {
			if task.state == task_unexecuted && len(m.idleSlaves) > 0 {
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
					e := TimerEvent{gloid: m.gloid, x: 3, needReply: false, timeout: work.mapTimeout}
					m.addTimerEvent(e)
					msg.Type = Message.Map
					msg.Data = []string{task.dataPath}
					msg.ExecFunc = work.mapExecPath
					msg.HashFunc = work.hashPath
				} else {
					if work.state == work_begin {
						m.blockSlavesNum++
						work.doReduceSlaves = append(work.doReduceSlaves, sid)
					} else if work.state == work_all_map_finished {
						e := TimerEvent{gloid: m.gloid, x: 3, needReply: false, timeout: work.reduceTimeout}
						m.addTimerEvent(e)
					}
					msg.Type = Message.Reduce
					msg.Data = work.finishedMap
					msg.ExecFunc = work.reduceExecPath
					msg.HashCode = task.hashCode
					msg.Wid = work.mapNum
				}
				m.toBottomChan <- msg
			}
			if len(m.idleSlaves) == 0 {
				return
			}
		}
	}
}

func (m *Master) Run() error {
	for {
		select {
		case msg, opened := <-m.fromBottomChan:
			if !opened {
				panic("msg chan closed")
			}
			if msg.Type == Message.ClientReply {
				m.processReplyFromClient(msg.Wid)
			} else if msg.Type == Message.Map {
				m.processMapFinish(msg.Gloid, msg.From, msg.Data[0])
			} else if msg.Type == Message.Reduce {
				m.processReduceFinish(msg.Gloid, msg.From, msg.Data[0])
			} else if msg.Type == Message.SlaveReply {
				m.processReplyFromSlave(msg.Gloid)
			} else if msg.Type == Message.NewWork {
				m.newWork(msg.ExecFunc, msg.ExecFunc2, msg.HashFunc, msg.Data, msg.HashCode, msg.From)
			}
		case gloid, opened := <-m.timerEventChan:
			if !opened {
				panic("timer event chan closed")
			}
			m.processTimeout(gloid)
		}
	}
}
