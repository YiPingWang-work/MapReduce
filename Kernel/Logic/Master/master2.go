package Master

import (
	"MapReduce_v0.1/Kernel/Message"
	"fmt"
	"log"
	"time"
)

func (m *Master) Init(myid int, slid []int,
	fromBottomChan <-chan Message.Message, toBottomChan chan<- Message.Message) {
	m.id = myid
	m.slaves = []*Slave{}
	m.idleSlaves = []int{}
	for _, v := range slid {
		m.slaves = append(m.slaves, &Slave{
			id:    v,
			gloid: -1,
			state: slave_idle,
		})
		m.idleSlaves = append(m.idleSlaves, v)
	}
	m.deadSlavesNum, m.blockSlavesNum = 0, 0
	m.works = map[int]*Work{}
	m.wid, m.gloid = 0, 0
	m.busy = map[int]Bind{}
	m.fromBottomChan, m.toBottomChan = fromBottomChan, toBottomChan
	m.timerEventChan = make(chan int, 100000)
	m.timerEventX = map[int]TimerEvent{}
	m.networkDelay = 3 * time.Second
}

func (m *Master) Run() error {
	for {
		select {
		case msg, opened := <-m.fromBottomChan:
			if !opened {
				log.Println("msg chan closed")
				fmt.Println(m.ToString())
				return nil
			}
			fmt.Println(m.ToString())
			if msg.Type == Message.ClientReply {
				m.processReplyFromClient(msg.Wid)
			} else if msg.Type == Message.Map {
				m.processMapFinish(msg.Gloid, msg.From, msg.DataPath[0])
			} else if msg.Type == Message.Reduce {
				m.processReduceFinish(msg.Gloid, msg.From, msg.DataPath[0])
			} else if msg.Type == Message.SlaveReply {
				m.processReplyFromSlave(msg.Gloid)
			} else if msg.Type == Message.NewWork {
				m.newWork(msg.Exec, msg.Exec2, msg.DataPath, msg.HashCode, msg.From)
			}
		case gloid, opened := <-m.timerEventChan:
			if !opened {
				log.Println("timer event chan closed")
				return nil
			}
			m.processTimeout(gloid)
		}
	}
}

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
	if bind.sid != sid {
		panic("bind error")
	}
	log.Printf("map finished, gloid: %d, %s\n", gloid, bind.ToString())
	delete(m.busy, gloid)
	delete(m.timerEventX, gloid)
	work := m.works[bind.wid]
	work.tasks[bind.tid].state = task_finished
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
			DataPath:  work.finishedMap,
			Exec:      work.reduceExec,
			HashCode:  work.tasks[m.busy[m.slaves[v].gloid].tid].hashCode,
		}
	}
	if len(work.finishedMap) == work.mapNum {
		log.Printf("all map finished, wid: %d\n", work.id)
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
	if bind.sid != sid {
		panic("bind error")
	}
	log.Printf("reduce finished, gloid: %d, %s\n", gloid, bind.ToString())
	delete(m.busy, gloid)
	delete(m.timerEventX, gloid)
	work := m.works[bind.wid]
	work.tasks[bind.tid].state = task_finished
	work.finishedReduce = append(work.finishedReduce, dataPath)
	m.slaves[sid].state = slave_idle
	m.idleSlaves = append(m.idleSlaves, sid)
	if len(work.finishedReduce) == work.reduceNum {
		work.state = work_finished
		log.Printf("work finished, wid: %d. reesult is %v\n", work.id, work.finishedReduce)
		m.toBottomChan <- Message.Message{
			From:      m.id,
			To:        work.client,
			NeedReply: true,
			Wid:       work.id,
			Type:      Message.ClientReply,
			DataPath:  work.finishedReduce,
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
	if e.x == 0 || e.needReply { // 丧失了执行能力，这个任务被作废
		log.Printf("timeout, task failed, gloid: %d, %v\n", gloid, bind.ToString())
		m.deadSlavesNum++
		m.works[bind.wid].tasks[bind.tid].state = task_unexecuted
		m.slaves[bind.sid].state = slave_dead
		delete(m.busy, e.gloid)
		m.schedule()
	} else {
		log.Printf("timout, slave need reply, gloid: %d, %v\n", e.gloid, bind.ToString())
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
			msg.DataPath = []string{task.dataPath}
			msg.Exec = work.mapExec
		} else {
			msg.Type = Message.Reduce
			msg.Wid = work.mapNum
			msg.Exec = work.reduceExec
			msg.DataPath = work.finishedMap
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
		log.Printf("slave reply. %v %v\n", bind, e)
		if m.works[bind.wid].tasks[bind.tid].kind == task_map {
			e.timeout = m.works[bind.wid].mapTimeout
		} else {
			e.timeout = m.works[bind.wid].reduceTimeout
		}
		m.addTimerEvent(e)
	}
}

func (m *Master) processReplyFromClient(wid int) {
	if _, has := m.works[wid]; has {
		// 要求数据库删除所有task保存数据的位置
		delete(m.works, wid)
	}
}

func (m *Master) newWork(mapExecPath, reduceExecPath string, data []string, hashCodeNum int, from int) {
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

func (m *Master) schedule() {
	if m.blockSlavesNum+m.deadSlavesNum == len(m.slaves) {
		log.Println("all alive slaves are block!")
		for gloid, bind := range m.busy {
			log.Printf("release a block slave, gloid: %d, %s\n", gloid, bind.ToString())
			m.blockSlavesNum--
			m.works[bind.wid].tasks[bind.tid].state = task_unexecuted
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
					msg.DataPath = []string{task.dataPath}
					msg.Exec = work.mapExec
					log.Printf("a new map task send to slave, gloid: %d, %v\n", m.gloid, m.busy[m.gloid].ToString())

				} else {
					if work.state == work_begin {
						m.blockSlavesNum++
						work.doReduceSlaves = append(work.doReduceSlaves, sid)
					} else if work.state == work_all_map_finished {
						e := TimerEvent{gloid: m.gloid, x: 3, needReply: false, timeout: work.reduceTimeout}
						m.addTimerEvent(e)
					}
					msg.Type = Message.Reduce
					msg.DataPath = work.finishedMap
					msg.Exec = work.reduceExec
					msg.HashCode = task.hashCode
					msg.Wid = work.mapNum
					log.Printf("a new reduce task send to slave, gloid: %d, %v\n", m.gloid, m.busy[m.gloid].ToString())

				}
				m.toBottomChan <- msg
			}
			if len(m.idleSlaves) == 0 {
				return
			}
		}
	}
}

func (m *Master) addTimerEvent(e TimerEvent) {
	m.timerEventX[e.gloid] = e
	go func() {
		time.Sleep(e.timeout)
		m.timerEventChan <- e.gloid
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
		res += fmt.Sprintf("  wid: %d, state: %s, mapNum: %d, reduceNum: %d, client: %d\n",
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
		res += fmt.Sprintf("  }\n  finishedMap: %v\n", v.finishedMap)
		res += fmt.Sprintf("  finishedReduce: %v\n", v.finishedReduce)
		res += fmt.Sprintf("  slave who block on reduce: %v\n", v.doReduceSlaves)
	}
	res += fmt.Sprintf("}\nrunninng tasks:\n{\n")
	for k, v := range m.busy {
		res += fmt.Sprintf("  gloid: %d, %s\n", k, v.ToString())
	}
	res += fmt.Sprintf("}\n=================")
	return res
}

func (b Bind) ToString() string {
	return fmt.Sprintf("wid: %d, tid: %d, sid: %d", b.wid, b.tid, b.sid)
}
