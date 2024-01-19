package Message

/*
定义消息类型
*/

const (
	Map    int = iota // map任务或map返回
	Reduce            // reduce任务或完成返回
	ClientReply
	SlaveReply
	NewWork
)

type Message struct {
	From      int      // 来自某个节点
	To        int      // 发给某个节点
	NeedReply bool     //是否需要回复
	Type      int      // 消息种类
	Gloid     int      // 任务编号
	Wid       int      // 作业编号（客户端返回使用），总Map数量（Reduce任务使用）
	Data      []string // 数据路径集合
	ExecFunc  string   // 执行文件的路径
	ExecFunc2 string   // 执行文件的路径（另一个函数，新任务使用）
	HashFunc  string   // Hash函数的路径（map使用）
	HashCode  int      // 哈希码（reduce使用）总哈希数量（新任务使用）
}
