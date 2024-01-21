package Message

import "fmt"

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
	Gloid     uint64   // 任务编号（master、slave任务的唯一凭证），map的执行时间ms（客户端新建任务时使用后32位）
	Wid       int      // 作业编号（客户端返回使用），总Map数量（Reduce任务使用），reduce的执行时间ms（客户端新建任务时使用）
	DataPath  []string // 数据路径集合（master发送为源数据路径，slave发送为结果数据路径）
	Exec      string   // 执行文件的路径（master发送slave声明的map路径或slave路径），map函数路径（客户端新建任务使用）
	Exec2     string   // reduce函数路径（客户端新建任务使用）
	HashCode  int      // 哈希码（reduce使用）总哈希数量（客户端新任务使用）
}

func (m Message) ToString() string {
	res := ""
	if m.Type == Map {
		res = fmt.Sprintf("map from %d to %d:: gloid: %d, dataPath: %v, mapFunc: %s",
			m.From, m.To, m.Gloid, m.DataPath, m.Exec)
	} else if m.Type == Reduce {
		res = fmt.Sprintf("reduce from %d to %d:: gloid: %d, dataPath: %v, reduceFunc: %s, HashCode: %d, mapNum: %d",
			m.From, m.To, m.Gloid, m.DataPath, m.Exec, m.HashCode, m.Wid)
	} else if m.Type == ClientReply {
		res = fmt.Sprintf("client finished from %d to %d:: wid: %d, client: %d", m.From, m.To, m.Wid, m.To)
	} else if m.Type == SlaveReply {
		res = fmt.Sprintf("slave reply from %d to %d:: gloid: %d", m.From, m.To, m.Gloid)
	} else if m.Type == NewWork {
		res = fmt.Sprintf("new work from %d to %d:: dataPath: %v, mapFunc: %s, reduceFunc: %s, totalHash: %d, mapTimeout: %d, reduceTimout: %d",
			m.From, m.To, m.DataPath, m.Exec, m.Exec2, m.HashCode, m.Gloid, m.Wid)
	} else {
		return "(illegal message type)"
	}
	if m.NeedReply {
		res += " (NEED REPLY)"
	}
	return res
}
