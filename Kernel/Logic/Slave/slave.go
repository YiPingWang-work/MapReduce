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
	lastTask       Task
	nowTask        Task
	remoteDir      string
	localDir       string
	fromBottomChan <-chan Message.Message
	taskChan       chan Message.Message
	toBottomChan   chan<- Message.Message
	timer          <-chan time.Time
	timeout        time.Duration
	masterId       int
}

type Task struct {
	gloid        uint64
	wid          int
	kind         int
	execFunc     string
	source       []string
	reduceSource map[string]bool
	result       string
	hashCode     int
	mapNum       int
}

/*
初始化函数，需要指明slave的节点ID（全局唯一）和slave定时时间（过期后给master发送验活报文）
*/

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
			if !(s.nowTask.gloid == 0 && s.lastTask.gloid <= msg.Gloid ||
				s.nowTask.gloid != 0 && s.nowTask.gloid <= msg.Gloid) {
				continue
			}
			if s.lastTask.gloid == msg.Gloid {
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
			if err := s.processReduce1(s.nowTask.execFunc, v, s.nowTask.result); err != nil {
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
map函数设计规范：输入为两个参数，map函数将处理名为第一个参数的文件，之后将结果放在名为第二个参数的目录下。
*/

func (s *Slave) processMap1(execFunc, sourceFile, resultDir string) error {
	log.Printf("Slave %d: map %s -> %s\n", s.id, sourceFile, resultDir)
	time.Sleep(time.Duration(len(sourceFile)*100+rand.Intn(10000)) * time.Millisecond)
	return nil
}

/*
reduce函数设计规范：输入为两个参数，reduce将读取第一个参数的文件和第二个参数的文件，之后将处理结果写入第二个文件（更新第二个文件的内容）。
*/

func (s *Slave) processReduce1(execFunc, sourceFile string, resultFile string) error {
	log.Printf("Slave %d: reduce %s -> %s\n", s.id, sourceFile, resultFile)
	time.Sleep(time.Duration(len(sourceFile)*100+rand.Intn(10000)) * time.Millisecond)
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
