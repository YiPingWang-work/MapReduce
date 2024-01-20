package Slave

import (
	"fmt"
	"log"
	"os/exec"
)

type Slave struct {
	id       int
	lastTask Task // 上一个成功执行的任务
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
	if t.gloid == s.lastTask.gloid { // 这个任务已经执行完毕了
		// 写管道
		return
	}
	execFunc, err := s.downloadAndSave(fmt.Sprintf("%d", t.gloid), "map_"+t.execFunc)
	// 这个函数要求新建（若没有）一个目录命名为gloid，之后将后面的文件从远程下载到本地的gloid目录下，之后返回可执行文件的地址
	if err != nil {
		log.Println(err)
		return
	}
	data, err := s.downloadAndSave(fmt.Sprintf("%d", t.gloid), "data_"+t.dataPath[0])
	// 这个函数要求新建（若没有）一个目录命名为gloid，之后将后面的文件从远程下载到本地的gloid目录下，之后返回数据文件的地址
	if err != nil {
		log.Println(err)
		return
	}
	resultDir, _ := s.downloadAndSave(fmt.Sprintf("%d/result", t.gloid), "")
	// 如果文件名为空，则表示建立一个目录名为gloid/result
	cmd := fmt.Sprintf("%s %s %s", execFunc, data, resultDir) // 要求map函数接受的参数是输入文件位置，输出文件夹位置，且内部按照哈希命名
	run := exec.Command("/bin/zsh", "-c", cmd)
	if err := run.Run(); err != nil {
		log.Println(err)
	}

	if err = s.upload(resultDir); err != nil {
		log.Println(err)
	}
	// 写管道
}

func (s *Slave) execReduce() {

}

func (s *Slave) downloadAndSave(gloid string, path string) (string, error) {
	return "", nil
}

func (s *Slave) upload(path string) error {
	return nil
}

func (s *Slave) removeFrom(path string) error {
	return nil
}
