package Logic

/*
处理Map任务的节点，处理Map的任务只需要保留主节点的信息就行
*/

type MapSlave struct {
	mapFunc string // 描述执行文件的位置
}

/*
处理Map任务，收到任务(Task)后进行处理，并将任务推送到内存（数组）的某个位置
*/

func (m *MapSlave) doMap() {

}

func (m *MapSlave) run() {

}
