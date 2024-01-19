package Message

/*
定义消息类型
*/

const (
	Map          int = iota // map任务或map返回
	Reduce                  // reduce返回
	ChangeReduce            // 修正自己的状态称为reduce状态
	AddReduce               // 新map任务完成，reduce需要异步同步
	ReduceCommit            // 最终reduce任务确定，处理完就可以返回
)

type Message struct {
	From       int
	To         int   // 发给某个节点
	Type       int   // 消息种类
	Wid        int   // 作业编号
	Tid        int   // 任务编号
	MasterAddr int   // master节点位置
	Addrs      []int // reduce任务时使用，都去哪里获得中间数据
}
