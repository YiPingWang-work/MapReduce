package Logic

import (
	"MapReduce_v0.1/Kernel/Logic/Master"
	"MapReduce_v0.1/Kernel/Logic/Slave"
	"MapReduce_v0.1/Kernel/Message"
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"
)

func TestLogic(t *testing.T) {
	mch1 := make(chan Message.Message, 1000)
	mch2 := make(chan Message.Message, 1000)
	sch01 := make(chan Message.Message, 1000)
	sch02 := make(chan Message.Message, 1000)
	sch11 := make(chan Message.Message, 1000)
	sch12 := make(chan Message.Message, 1000)
	sch21 := make(chan Message.Message, 1000)
	sch22 := make(chan Message.Message, 1000)
	sch31 := make(chan Message.Message, 1000)
	sch32 := make(chan Message.Message, 1000)
	go fakeRoute(mch1, mch2, sch01, sch02, sch11, sch12, sch21, sch22, sch31, sch32, time.After(462*time.Second))
	go func() {
		m := Master.Master{}
		m.Init(0, []int{0, 1, 2, 3}, 3, 4, 8000, mch2, mch1)
		m.Run()
		log.Println("over")
	}()
	//go func() {
	//	m := Slave.Slave{}
	//	m.Init(0, 100000, sch02, sch01)
	//	m.Run()
	//	log.Println("over")
	//}()
	go func() {
		m := Slave.Slave{}
		m.Init(1, 100000, sch12, sch11)
		m.Run()
		log.Println("over")
	}()
	go func() {
		m := Slave.Slave{}
		m.Init(2, 100000, sch22, sch11)
		m.Run()
		log.Println("over")
	}()
	go func() {
		m := Slave.Slave{}
		m.Init(3, 100000, sch32, sch31)
		m.Run()
		log.Println("over")
	}()
	time.Sleep(time.Second)
	mch2 <- Message.Message{
		From:     12,
		To:       0,
		Type:     Message.NewWork,
		DataPath: []string{"hello", "me", "crut"},
		Exec:     "domap",
		Exec2:    "doreduce",
		HashCode: 2,
		Gloid:    15000,
		Wid:      15000,
	}
	time.Sleep(time.Second)
	mch2 <- Message.Message{
		From:     12,
		To:       0,
		Type:     Message.NewWork,
		DataPath: []string{"uio", "zip"},
		Exec:     "domap",
		Exec2:    "doreduce",
		HashCode: 3,
		Gloid:    15000,
		Wid:      15000,
	}
	time.Sleep(20 * time.Second)
	mch2 <- Message.Message{
		From:     12,
		To:       0,
		Type:     Message.NewWork,
		DataPath: []string{"tim", "oppsuioo"},
		Exec:     "domap",
		Exec2:    "doreduce",
		HashCode: 2,
		Gloid:    15000,
		Wid:      20000,
	}
	time.Sleep(500 * time.Second)
	closeAndOutput(mch1)
	closeAndOutput(mch2)
	closeAndOutput(sch01)
	closeAndOutput(sch02)
	closeAndOutput(sch11)
	closeAndOutput(sch12)
	closeAndOutput(sch21)
	closeAndOutput(sch22)
	time.Sleep(time.Second)
}

func closeAndOutput(ch chan Message.Message) {
	close(ch)
	for v := range ch {
		fmt.Println(v.ToString())
	}
	fmt.Println()
}

/*
模拟路由
*/

func fakeRoute(mch1, mch2, sch01, sch02, sch11, sch12, sch21,
	sch22, sch31, sch32 chan Message.Message, finished <-chan time.Time) {
	rand.Seed(time.Now().UnixNano())
	for {
		select {
		case msg := <-mch1:
			if msg.To == 0 {
				go func() {
					t := rand.Intn(40)
					fmt.Println("--> delay to 0:", t)
					time.Sleep(time.Duration(t) * time.Second)
					sch02 <- msg
				}()
			} else if msg.To == 1 {
				go func() {
					t := rand.Intn(40)
					fmt.Println("--> delay to 1:", t)
					time.Sleep(time.Duration(t) * time.Second)
					sch12 <- msg
				}()
			} else if msg.To == 2 {
				go func() {
					t := rand.Intn(40)
					fmt.Println("--> delay to 2:", t)
					time.Sleep(time.Duration(t) * time.Second)
					sch22 <- msg
				}()
			} else if msg.To == 3 {
				go func() {
					t := rand.Intn(40)
					fmt.Println("--> delay to 3:", t)
					time.Sleep(time.Duration(t) * time.Second)
					sch32 <- msg
				}()
			} else {
				fmt.Println("* <-- ", msg.ToString())
			}
		case msg := <-sch01:
			mch2 <- msg
		case msg := <-sch11:
			mch2 <- msg
		case msg := <-sch21:
			mch2 <- msg
		case msg := <-sch31:
			mch2 <- msg
		case <-finished:
			return
		}
	}
}
