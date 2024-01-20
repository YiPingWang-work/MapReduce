package Slave

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
)

type Slave struct {
	id        int
	lastTask  Task // 上一个成功执行的任务
	remoteDir string
	localDir  string
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
	execFunc, err := s.downloadAndSave(fmt.Sprintf("%d", t.gloid), t.execFunc)
	// 这个函数要求新建（若没有）一个目录命名为gloid，之后将后面的文件从远程下载到本地的gloid目录下，之后返回可执行文件的地址
	if err != nil {
		log.Println(err)
		return
	}
	data, err := s.downloadAndSave(fmt.Sprintf("%d", t.gloid), t.dataPath[0])
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

	//if err = s.upload(resultDir); err != nil {
	//	log.Println(err)
	//}
	// 写管道
}

func (s *Slave) execReduce() {

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
