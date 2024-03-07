package Master

import (
	"testing"
	"time"
)

import (
	"container/heap"
	"fmt"
	"log"
	"sync"
)

type TimerEvent_test struct {
	time time.Time
	fn   func(agrs ...interface{})
	id   uint64
}

type TimerHeap []TimerEvent_test

func (t TimerHeap) Len() int {
	return len(t)
}

func (t TimerHeap) Less(i, j int) bool {
	return t[i].time.UnixNano() < t[j].time.UnixNano()
}

func (t TimerHeap) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t *TimerHeap) Push(x any) {
	*t = append(*t, x.(TimerEvent_test))
}

func (t *TimerHeap) Pop() any {
	x := (*t)[len(*t)-1]
	*t = (*t)[:len(*t)-1]
	return x
}

func (t TimerHeap) Top() any {
	return t[0]
}

var _ heap.Interface = (*TimerHeap)(nil)

type Timers struct {
	timer *time.Timer
	heap  TimerHeap
	has   map[uint64]struct{}
	nowId uint64
	m     sync.Mutex
}

func (t *Timers) Register(after time.Duration, f func(args ...interface{})) uint64 {
	t.m.Lock()
	t.nowId++
	id := t.nowId
	t.has[t.nowId] = struct{}{}
	newEvent := TimerEvent_test{
		time: time.Now().Add(after),
		fn:   f,
		id:   id,
	}
	if newEvent.time.UnixNano() < t.heap.Top().(TimerEvent_test).time.UnixNano() {
		fmt.Println("change")
		//if !t.timer.Stop() { // 如果是已经关闭的timer，则要去拿走一个
		//	<-t.timer.C
		//}
		t.timer.Reset(after)
	}
	heap.Push(&t.heap, newEvent)
	t.m.Unlock()
	return id
}

func (t *Timers) Delete(id uint64) {
	t.m.Lock()
	delete(t.has, id)
	t.m.Unlock()
}

func (t *Timers) Listen() {
	for {
		select {
		case <-t.timer.C:
			t.m.Lock()
			for {
				top := heap.Pop(&t.heap).(TimerEvent_test)
				if _, ok := t.has[top.id]; ok {
					delete(t.has, top.id)
					top.fn()
				}
				if time.Now().UnixNano() < t.heap.Top().(TimerEvent_test).time.UnixNano() {
					t.timer.Reset(t.heap.Top().(TimerEvent_test).time.Sub(time.Now()))
					break
				}
			}
			t.m.Unlock()
		}
	}
}

func (t *Timers) Init() {
	t.has = map[uint64]struct{}{}
	heap.Push(&t.heap, TimerEvent_test{time: time.Now().Add(1e5 * time.Hour)})
	t.timer = time.NewTimer(1e5 * time.Hour)
}

func TestTimeHeaper(t *testing.T) {
	timer := Timers{}
	timer.Init()
	go timer.Listen()
	log.Println()
	for i := 0; i < 1000; i++ {
		i := i
		timer.Register(time.Second, func(args ...interface{}) {
			log.Println("after 1 --", i)
		})
	}
	timer.Register(3*time.Second, func(args ...interface{}) {
		log.Println("after 3")
	})
	timer.Register(4*time.Second, func(args ...interface{}) {
		log.Println("after 4")
	})
	res := timer.Register(5*time.Second, func(args ...interface{}) {
		log.Println("after 5")
	})
	time.Sleep(10 * time.Second)
	timer.Register(10*time.Second, func(args ...interface{}) {
		log.Println("after 15")
	})
	timer.Delete(res)
	time.Sleep(20 * time.Second)
}
