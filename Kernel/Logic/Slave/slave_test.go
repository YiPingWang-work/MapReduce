package Slave

import (
	"fmt"
	"log"
	"os"
	"os/exec"
	"testing"
)

func TestSlave(t *testing.T) {
	s := Slave{
		localDir:  "/Users/yipingwang/Desktop/data1",
		remoteDir: "/Users/yipingwang/Desktop/data",
	}
	err := s.load(s.remoteDir+"/"+"B", s.localDir+"/"+"1999/", "B")
	if err != nil {
		log.Println(err)
	}
	cmd := exec.Command(fmt.Sprintf("%s", s.localDir+"/"+"1999/"+"B"))
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	_ = cmd.Run()
}
