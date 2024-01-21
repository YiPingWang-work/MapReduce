package Master

import (
	"MapReduce_v0.1/Kernel/Message"
	"fmt"
	"log"
	"testing"
	"time"
)

func TestMaster(test *testing.T) {
	ch1 := make(chan Message.Message, 100)
	ch2 := make(chan Message.Message, 100)
	go func() {
		m := Master{}
		m.Init(0, []int{0, 1, 2}, 3, 4, 1000, ch1, ch2)
		m.Run()
		log.Println("over")
	}()
	time.Sleep(time.Second)
	ch1 <- Message.Message{
		From:     12,
		To:       0,
		Type:     Message.NewWork,
		DataPath: []string{"hello", "hi", "zip"},
		Exec:     "domap",
		Exec2:    "doreduce",
		HashCode: 2,
		Gloid:    5000,
		Wid:      10000,
	}
	time.Sleep(300 * time.Second)
	close(ch1)
	close(ch2)
	time.Sleep(time.Second)
	for v := range ch2 {
		fmt.Println(v.ToString())
	}
}
