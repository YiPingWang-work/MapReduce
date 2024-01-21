package Slave

import (
	"MapReduce_v0.1/Kernel/Message"
	"fmt"
	"io/ioutil"
	"log"
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

/*
给出执行函数，要操作的文件位置，最终结果存放的目录位置，sourceFile是远端数据，需要建立TCP长链接拉取，
它将把文件保存为：最终存放目录/Hash码，这个目录和这个数据是在本地
*/

func (s *Slave) processMap(execFunc, sourceFile, resultDir string) error {
	log.Printf("slave: map %s\n", sourceFile)
	time.Sleep(time.Duration(len(sourceFile)) * time.Second)
	return nil
}

/*
给出执行函数，要操作的文件夹位置，这个reduce任务的哈希码，最终存放的文件位置，sourceDir/HashCode这个数据是在远端，需要建立TCP长链接获取
它将取操作的文件夹/Hash码这个文件。并顺序追加写resultFile，这个文件在本地
*/

func (s *Slave) processReduce(execFunc, sourceDir string, hashCode int, resultFile string) error {
	log.Printf("slave: reduce %s\n", sourceDir)
	time.Sleep(time.Duration(len(sourceDir)) * time.Second)
	return nil
}

func (s *Slave) downloadAndSave(dir string, file string) (string, error) {
	dir = s.localDir + "/" + dir
	if _, err := os.Stat(dir); err != nil {
		log.Printf("create dir %s\n", dir)
		if err := os.MkdirAll(dir, os.ModePerm); err != nil {
			return "", err
		}
	}
	if file == "" {
		return dir, nil
	}
	remoteFile := s.remoteDir + "/" + file
	file = dir + "/" + file
	if _, err := os.Stat(file); err != nil {
		log.Printf("create file %s and get data from remote file %s\n", file, remoteFile)
		content, err := ioutil.ReadFile(remoteFile)
		if err != nil {
			return "", err
		}
		err = ioutil.WriteFile(file, content, 0777)
		if err != nil {
			return "", err
		}
	}
	return file, nil
}

//

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

func (s *Slave) removeFrom(path string) error {
	return nil
}

func (s *Slave) daemon() {
	for {
		select {
		case msg, opened := <-s.fromBottomChan:
			if !opened {
				log.Println("slave: msg chan closed")
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

func (s *Slave) Run() error {
	s.timer = time.After(s.timeout)
	go s.daemon()
	for {
		select {
		case msg, opened := <-s.taskChan:
			if !opened {
				log.Println("slave: task chan closed")
				return nil
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
				s.nowTask = Task{
					gloid:    msg.Gloid,
					kind:     Message.Map,
					execFunc: msg.Exec,
					source:   msg.DataPath,
					result:   "M$" + msg.DataPath[0],
				}
				if err := s.processMap("", s.nowTask.source[0], ""); err != nil { // 拉取数据，保存到本地，处理数据，保存到本地
					log.Println(err)
				} else {
					// 推送数据到远端
					s.toBottomChan <- Message.Message{
						From:     s.id,
						To:       s.masterId,
						Type:     Message.Map,
						Gloid:    msg.Gloid,
						DataPath: []string{s.nowTask.result},
					}
					s.timer = time.After(s.timeout)
					// 删除lastTask在本地保存的数据
					s.lastTask = s.nowTask
				}
				s.nowTask = Task{gloid: 0}
			} else if msg.Type == Message.Reduce {
				if s.nowTask.gloid == msg.Gloid { // 收到的信息是本次任务的ID，因为Reduce很可能阻塞
					for _, v := range msg.DataPath {
						if _, has := s.nowTask.reduceSource[v]; !has {
							s.nowTask.reduceSource[v] = true
							if err := s.processReduce("", v, msg.HashCode, ""); err != nil { // 拉取数据到本地，处理，保存到本地
								log.Println(err)
							} else {
								s.nowTask.result += v
							}
						}
					}
					if len(msg.DataPath) == msg.Wid { // 所有reduce任务都已经完成了
						// 将本地数据推送到远端
						s.toBottomChan <- Message.Message{
							From:     s.id,
							To:       s.masterId,
							Type:     Message.Reduce,
							Gloid:    msg.Gloid,
							DataPath: []string{s.nowTask.result},
						}
						s.timer = time.After(s.timeout)
						// 删除本次数据
						s.lastTask = s.nowTask
						s.nowTask = Task{gloid: 0}
					}
				} else { // reduce新任务，此时放弃正在执行的任务，处理新任务
					s.nowTask = Task{
						gloid:        msg.Gloid,
						kind:         0,
						execFunc:     msg.Exec,
						source:       msg.DataPath,
						result:       "R$" + fmt.Sprintf("%d", msg.Gloid),
						reduceSource: map[string]bool{},
						hashCode:     msg.HashCode,
						mapNum:       msg.Wid,
					}
					for _, v := range msg.DataPath {
						s.nowTask.reduceSource[v] = true
						if err := s.processReduce("", v, msg.HashCode, ""); err != nil { // 拉取数据到本地，处理，保存到本地
							log.Println(err)
						} else {
							s.nowTask.result += v
						}
					}
				}
			}
		case <-s.timer:
			log.Println("slave: check alive")
			s.toBottomChan <- Message.Message{
				From:  s.id,
				To:    s.masterId,
				Type:  Message.Map,
				Gloid: 0,
			}
			s.timer = time.After(s.timeout)
		}
	}
}

func (s *Slave) Init(id int, timeout int, fromBottomChan <-chan Message.Message, toBottomChan chan<- Message.Message) {
	s.id, s.timeout = id, time.Duration(timeout)*time.Millisecond
	s.fromBottomChan, s.toBottomChan = fromBottomChan, toBottomChan
	s.taskChan = make(chan Message.Message, 10000)
	s.lastTask = Task{}
	s.nowTask = Task{}

}
