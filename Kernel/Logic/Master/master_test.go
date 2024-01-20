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
		m.Init(0, []int{0, 1, 2}, ch1, ch2)
		m.Run()
		log.Println("over")
	}()
	time.Sleep(time.Second)
	ch1 <- Message.Message{
		From:     12,
		To:       0,
		Type:     Message.NewWork,
		DataPath: []string{"hello", "hi"},
		Exec:     "domap",
		Exec2:    "doreduce",
		HashCode: 2,
	}
	time.Sleep(3 * time.Second)
	ch1 <- Message.Message{
		From:     0,
		To:       0,
		Type:     Message.Map,
		Gloid:    1,
		DataPath: []string{"1"},
	}
	//ch1 <- Message.Message{
	//	From:     13,
	//	To:       0,
	//	Type:     Message.NewWork,
	//	DataPath: []string{"zip", "ui", "t"},
	//	Exec:     "domap",
	//	Exec2:    "doreduce",
	//	HashCode: 2,
	//}
	time.Sleep(1 * time.Second)
	ch1 <- Message.Message{
		From:     1,
		To:       0,
		Type:     Message.Map,
		Gloid:    2,
		DataPath: []string{"2"},
	}
	time.Sleep(3 * time.Second)
	ch1 <- Message.Message{
		From:     2,
		To:       0,
		Type:     Message.Reduce,
		Gloid:    3,
		DataPath: []string{"3"},
	}
	ch1 <- Message.Message{
		From:     0,
		To:       0,
		Type:     Message.Reduce,
		Gloid:    4,
		DataPath: []string{"4"},
	}
	time.Sleep(5 * time.Second)
	close(ch1)
	close(ch2)
	for v := range ch2 {
		fmt.Println(v)
	}
}
