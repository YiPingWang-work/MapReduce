package Message

/*
定义消息类型
*/

const (
	Prepare int = iota
	PrepareReply
	Map
	MapReply
	MapData
	Reduce
	LastReduce
)

type Message struct {
	Type       int      // 消息种类
	Index      int      // 标识消息序列号，slave节点不应该接受任期号比其小的任务
	MasterAddr string   // master节点位置
	Addrs      []string // slave节点位置
}
