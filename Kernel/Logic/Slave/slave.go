package Slave

import (
	"MapReduce_v0.1/Kernel/Message"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"time"
)

type Slave struct {
	id             int
	lastTask       Task // 上一个成功执行的任务
	nowTask        Task // 现在正在执行的任务（仅Reduce使用）
	remoteDir      string
	localDir       string
	fromBottomChan <-chan Message.Message
	taskChan       chan Message.Message
	toBottomChan   chan<- Message.Message
	timer          <-chan time.Time
	timeout        time.Duration
	masterId       int // master节点信息
}

type Task struct {
	gloid        uint64
	wid          int
	kind         int
	execFunc     string
	source       []string        // 数据源路径
	reduceSource map[string]bool // reduce任务的执行表
	result       string          // 结果数据路径或路径前缀
	hashCode     int
	mapNum       int
}

func (s *Slave) Init(id int, timeout int,
	fromBottomChan <-chan Message.Message, toBottomChan chan<- Message.Message) {
	s.id, s.timeout = id, time.Duration(timeout)*time.Millisecond
	s.fromBottomChan, s.toBottomChan = fromBottomChan, toBottomChan
	s.taskChan = make(chan Message.Message, 10000)
	s.lastTask = Task{}
	s.nowTask = Task{}

}

/*
这个函数执行的时候，当它可以处理一条新信息的时候，状态只可能为以下三种：
1.lastTask为Map，此时空闲
2.lastTask为Reduce，此时空想
3.lastTask为Map或Reduce，此时为Reduce
且3状态下lastTask的gloid一定低于nowTask的gloid
函数只会接收gloid递增的命令，对其它命令直接拒绝。
*/

func (s *Slave) Run() {
	s.timer = time.After(s.timeout)
	go s.daemon()
	for {
		select {
		case msg, opened := <-s.taskChan:
			if !opened {
				log.Printf("Slave %d: task chan closed\n", s.id)
				return
			}
			if s.lastTask.gloid == msg.Gloid { // 如果是上一个自己执行过的任务，立即返回一个任务已经完成
				s.toBottomChan <- Message.Message{
					From:     s.id,
					To:       s.masterId,
					Type:     msg.Type,
					Gloid:    msg.Gloid,
					Wid:      msg.Wid,
					DataPath: []string{s.lastTask.result},
				}
				s.timer = time.After(s.timeout)
				continue
			}
			if s.lastTask.gloid > msg.Gloid || s.nowTask.gloid > msg.Gloid {
				continue
			}
			if msg.Type == Message.Map {
				/*
					对于一个Map任务，将接收的dataPath[0]，就是数据源头.
					map的结果是一系列按照Hash序区分好的文件集合，它将统一将这些文件存放到名为"Map$Gloid$sourcepath"的目录中，并返回目录地址。
					假设文件来源为AB，此次task的gloid为112，HashCode总数为3，则Map任务生成的所有文件地址为：
					Map$112$(AB)/0, Map$112$(AB)/1, Map$112$(AB)/2。该任务的返回结果为Map$112$(AB)。
				*/
				s.nowTask = Task{
					gloid:    msg.Gloid,
					kind:     Message.Map,
					execFunc: msg.Exec,
					source:   msg.DataPath,
					result:   fmt.Sprintf("Map$%d$%s", msg.Gloid, msg.DataPath[0]),
				}
				if err := s.processMap(); err != nil {
					log.Println(err)
				} else {
					s.taskFinished()
				}
			} else if msg.Type == Message.Reduce {
				if s.nowTask.gloid == msg.Gloid { // 收到的信息是本次任务的ID，因为Reduce很可能阻塞
					if finished, err := s.processReduce(msg.DataPath); err != nil {
						log.Println(err)
					} else if finished {
						s.taskFinished()
					}
				} else {
					/*
						此时为reduce新任务，slave将放弃正在执行的任务，处理新任务。
						对于一个Reduce任务，将接收整个dataPath数组并幂等维护，这个数组里携带所有map任务输出的目录。
						reduce任务将依次遍历这些目录，拉取每个目录下文件名为HashCode的文件，处理并追加到自己的结果文件中。
						reduce任务的结果文件命名为"Reduce$Gloid$hashCode"
					*/
					s.nowTask = Task{
						gloid:        msg.Gloid,
						kind:         Message.Reduce,
						execFunc:     msg.Exec,
						source:       msg.DataPath,
						result:       fmt.Sprintf("Reduce$%d$%d", msg.Gloid, msg.HashCode),
						reduceSource: map[string]bool{},
						hashCode:     msg.HashCode,
						mapNum:       msg.Wid,
					}
					if finished, err := s.processReduce([]string{}); err != nil {
						log.Println(err)
					} else if finished {
						s.taskFinished()
					}
				}
			}
		case <-s.timer:
			if err := s.processTimeout(); err != nil {
				log.Println(err)
			}
		}
	}
}

func (s *Slave) daemon() {
	for {
		select {
		case msg, opened := <-s.fromBottomChan:
			if !opened {
				log.Printf("Slave %d: msg chan closed\n", s.id)
				close(s.taskChan)
				return
			}
			if msg.To != s.id {
				continue
			}
			if msg.NeedReply {
				s.toBottomChan <- Message.Message{
					From:  s.id,
					To:    s.masterId,
					Type:  Message.SlaveReply,
					Gloid: msg.Gloid,
				}
				s.timer = time.After(s.timeout)
			}
			s.taskChan <- msg
		}
	}
}

func (s *Slave) processMap() error {
	if err := s.processMap1(s.nowTask.execFunc, s.nowTask.source[0], s.nowTask.result); err != nil {
		log.Println(err)
	}
	return nil
}

func (s *Slave) processReduce(sources []string) (bool, error) {
	for _, v := range sources {
		if _, has := s.nowTask.reduceSource[v]; !has {
			s.nowTask.reduceSource[v] = true
			if err := s.processReduce1(s.nowTask.execFunc, v, s.nowTask.hashCode, s.nowTask.result); err != nil {
				return false, err
			} else {
				s.nowTask.result += fmt.Sprintf("(%s)", v)
			}
		}
	}
	if len(s.nowTask.reduceSource) == s.nowTask.mapNum {
		return true, nil
	} else {
		return false, nil
	}
}

func (s *Slave) processTimeout() error {
	log.Printf("Slave %d: check alive\n", s.id)
	s.toBottomChan <- Message.Message{
		From:     s.id,
		To:       s.masterId,
		Type:     Message.Map,
		Gloid:    0,
		DataPath: []string{""},
	}
	s.timer = time.After(s.timeout)
	return nil
}

func (s *Slave) taskFinished() {
	s.toBottomChan <- Message.Message{
		From:     s.id,
		To:       s.masterId,
		Type:     s.nowTask.kind,
		Gloid:    s.nowTask.gloid,
		DataPath: []string{s.nowTask.result},
	}
	s.timer = time.After(s.timeout)
	s.removeTask(s.lastTask)
	s.lastTask = s.nowTask
	s.nowTask = Task{gloid: 0}
}

func (s *Slave) removeTask(t Task) {
	/*
		删除本地t的所有数据。
	*/
}

/*
给出执行函数，要操作的文件位置，最终结果存放的目录位置，sourceFile是远端数据，需要建立TCP长链接拉取，
它将把文件保存为：最终存放目录/Hash码，这个目录和这个数据是在本地
*/

func (s *Slave) processMap1(execFunc, sourceFile, resultDir string) error {
	log.Printf("Slave %d: map %s -> %s\n", s.id, sourceFile, resultDir)
	time.Sleep(time.Duration(len(sourceFile)*100+rand.Intn(10000)) * time.Millisecond)
	return nil
}

/*
给出执行函数，要操作的文件夹位置，这个reduce任务的哈希码，最终存放的文件位置，sourceDir/HashCode这个数据是在远端，需要建立TCP长链接获取
它将取操作的文件夹/Hash码这个文件。并顺序追加写resultFile，这个文件在本地
*/

func (s *Slave) processReduce1(execFunc, sourceDir string, hashCode int, resultFile string) error {
	log.Printf("Slave %d: reduce %s -> %s\n", s.id, sourceDir, resultFile)
	time.Sleep(time.Duration(len(sourceDir)*100+rand.Intn(10000)) * time.Millisecond)
	return nil
}

func (s *Slave) load(from, toDir, fileName string) error {
	if _, err := os.Stat(toDir + "/" + fileName); err != nil {
		log.Printf("create file %s and get data from remote file %s\n", toDir+"/"+fileName, from)
		content, err := ioutil.ReadFile(from)
		if err != nil {
			return err
		}
		err = os.MkdirAll(toDir, 0777)
		if err != nil {
			return err
		}
		err = ioutil.WriteFile(toDir+"/"+fileName, content, 0777)
		if err != nil {
			return err
		}
	}
	return nil
}

//func (s *Slave) Run2() {
//	s.timer = time.After(s.timeout)
//	go s.daemon()
//	for {
//		select {
//		case msg, opened := <-s.taskChan:
//			if !opened {
//				log.Printf("Slave %d: task chan closed\n", s.id)
//				return
//			}
//			if s.lastTask.gloid == msg.Gloid { // 如果是上一个自己执行过的任务，立即返回一个任务已经完成
//				s.toBottomChan <- Message.Message{
//					From:     s.id,
//					To:       s.masterId,
//					Type:     msg.Type,
//					Gloid:    msg.Gloid,
//					Wid:      msg.Wid,
//					DataPath: []string{s.lastTask.result},
//				}
//				s.timer = time.After(s.timeout)
//				continue
//			}
//			if s.lastTask.gloid > msg.Gloid || s.nowTask.gloid > msg.Gloid {
//				continue
//			}
//			if msg.Type == Message.Map {
//				s.nowTask = Task{
//					gloid:    msg.Gloid,
//					kind:     Message.Map,
//					execFunc: msg.Exec,
//					source:   msg.DataPath,
//					result:   "M$" + msg.DataPath[0],
//				}
//				if err := s.processMap1("", s.nowTask.source[0], ""); err != nil { // 拉取数据，保存到本地，处理数据，保存到本地
//					log.Println(err)
//				} else {
//					// 推送数据到远端
//					s.toBottomChan <- Message.Message{
//						From:     s.id,
//						To:       s.masterId,
//						Type:     Message.Map,
//						Gloid:    msg.Gloid,
//						DataPath: []string{s.nowTask.result},
//					}
//					s.timer = time.After(s.timeout)
//					// 删除lastTask在本地保存的数据
//					s.lastTask = s.nowTask
//				}
//				s.nowTask = Task{gloid: 0}
//			} else if msg.Type == Message.Reduce {
//				if s.nowTask.gloid == msg.Gloid { // 收到的信息是本次任务的ID，因为Reduce很可能阻塞
//					for _, v := range msg.DataPath {
//						if _, has := s.nowTask.reduceSource[v]; !has {
//							s.nowTask.reduceSource[v] = true
//							if err := s.processReduce1("", v, msg.HashCode, ""); err != nil { // 拉取数据到本地，处理，保存到本地
//								log.Println(err)
//							} else {
//								s.nowTask.result += v
//							}
//						}
//					}
//					if len(msg.DataPath) == msg.Wid { // 所有reduce任务都已经完成了
//						// 将本地数据推送到远端
//						s.toBottomChan <- Message.Message{
//							From:     s.id,
//							To:       s.masterId,
//							Type:     Message.Reduce,
//							Gloid:    msg.Gloid,
//							DataPath: []string{s.nowTask.result},
//						}
//						s.timer = time.After(s.timeout)
//						// 删除本次数据
//						s.lastTask = s.nowTask
//						s.nowTask = Task{gloid: 0}
//					}
//				} else { // reduce新任务，此时放弃正在执行的任务，处理新任务
//					s.nowTask = Task{
//						gloid:        msg.Gloid,
//						kind:         Message.Reduce,
//						execFunc:     msg.Exec,
//						source:       msg.DataPath,
//						result:       "R$" + fmt.Sprintf("%d", msg.Gloid),
//						reduceSource: map[string]bool{},
//						hashCode:     msg.HashCode,
//						mapNum:       msg.Wid,
//					}
//					for _, v := range msg.DataPath {
//						s.nowTask.reduceSource[v] = true
//						if err := s.processReduce1("", v, msg.HashCode, ""); err != nil { // 拉取数据到本地，处理，保存到本地
//							log.Println(err)
//						} else {
//							s.nowTask.result += v
//						}
//					}
//				}
//			}
//		case <-s.timer:
//			log.Printf("Slave %d: check alive\n", s.id)
//			s.toBottomChan <- Message.Message{
//				From:     s.id,
//				To:       s.masterId,
//				Type:     Message.Map,
//				Gloid:    0,
//				DataPath: []string{""},
//			}
//			s.timer = time.After(s.timeout)
//		}
//	}
//}
