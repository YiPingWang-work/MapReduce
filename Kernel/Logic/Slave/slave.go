package Slave

import (
	"fmt"
	"log"
	"os/exec"
)

type Slave struct {
	id       int
	lastTask Task // 上一个成功执行的任务
	nowTask  Task // 正在执行的任务
}

type Task struct {
	gloid      int // -1表示现在没有任务执行
	kind       int
	execFunc   string
	dataPath   []string
	resultPath []string
	hashCode   int
	mapNum     int
}

func (s *Slave) execMap(t Task) {
	execFunc, err := s.downloadAndSave(t.gloid, t.execFunc)
	if err != nil {
		log.Println(err)
		return
	}
	for _, v := range t.dataPath {
		data, err := s.downloadAndSave(t.gloid, v)
		if err != nil {
			log.Println(err)
			return
		}
		cmd := fmt.Sprintf("%s %s ", execFunc, data)
		run := exec.Command("/bin/zsh", "-c", cmd)
		if err := run.Run(); err != nil {
			log.Println(err)
		}

	}
}

func (s *Slave) execReduce() {

}

func (s *Slave) downloadAndSave(gloid int, path string) (string, error) {
	return "", nil
}

func (s *Slave) upload(path string) error {
	return nil
}

func (s *Slave) removeFrom(path string) error {
	return nil
}
