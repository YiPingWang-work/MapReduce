package Slave

type Slave struct {
	id       int
	lastTask Task
	nowTask  Task
}

type Task struct {
	gloid      int
	kind       int
	execFunc   string
	hashFunc   string
	dataPath   string
	resultPath []string
	hashCode   int
	mapNum     int
}
