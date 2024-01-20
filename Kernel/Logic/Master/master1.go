package Master

import (
	"MapReduce_v0.1/Kernel/Message"
	"time"
)

// work 的 state字段值
const (
	work_begin int = iota
	work_all_map_finished
	work_finished
)

// task 的 state字段值
const (
	task_unexecuted int = iota
	task_executing
	task_finished
)

// task 的 kind字段值
const (
	task_map int = iota
	task_reduce
)

// slave的state字段
const (
	slave_idle int = iota
	slave_working
	slave_dead
)

type Master struct {
	id             int                    // master的id
	slaves         []*Slave               // 所有的奴隶节点，它的id就是数组下标
	idleSlaves     []int                  // 处于空闲状态的奴隶节点
	deadSlavesNum  int                    // 死亡的节点个数
	blockSlavesNum int                    // 阻塞的节点个数
	works          map[int]*Work          // 所有的工作
	wid            int                    // 下一个全局workID
	gloid          int                    // 下一个全局event ID
	busy           map[int]Bind           // 系统中正在运行的所有任务
	fromBottomChan <-chan Message.Message // 接受消息管道
	toBottomChan   chan<- Message.Message // 发送消息管道
	timerEventChan chan int               // 定时事件管道
	timerEventX    map[int]TimerEvent     // 定时事件是否有效
	networkDelay   time.Duration          // 网络最高延迟
}

type Bind struct {
	wid int
	tid int
	sid int
}

type Slave struct {
	id    int // 奴隶id
	gloid int // 当前正在处理的任务，仅当state = slave_working时有效
	state int // 所处的状态
}

type Work struct {
	id             int           // 工作id
	tasks          []*Task       // 该工作下分的所有任务，它的id就是数组下标
	state          int           // 工作所处的状态
	finishedMap    []string      // map处理后数据地址，不包含Hash码，例如map返回的是noiacia，hash码从0到99，那么hash码是54的reduce任务需要获取的是：noiacia_54的数据
	finishedReduce []string      // 完成的reduce任务的路径，里面有完整的hash码信息，例如作业A的hash码是54的reduce任务执行结果为A_54
	doReduceSlaves []int         // 正在阻塞等待reduce任务的下标
	mapNum         int           // 该作业一共有多少个map任务
	reduceNum      int           // 该作业有多少个reduce任务
	mapTimeout     time.Duration // map超时事件
	reduceTimeout  time.Duration // reduce超时事件
	client         int           // 客户端节点
	mapExec        string        // map函数的地址
	reduceExec     string        // reduce函数的地址
}

type Task struct {
	id       int    // 任务的id
	kind     int    // 任务类型
	state    int    // 该任务的状态
	dataPath string // 数据文件的地址（仅map使用，是map处理文件的地址）
	hashCode int    // 哈希值（仅对reduce任务有效）
}

type TimerEvent struct {
	gloid     int           // 事件的唯一ID
	x         int           // 允许再次尝试的次数
	needReply bool          // 是否该事件需要回复
	timeout   time.Duration // 事件的允许时长
}
