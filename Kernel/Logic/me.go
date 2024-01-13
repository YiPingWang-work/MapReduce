package Logic

type Me struct {
	role Role
}

type Role interface {
	run()
}
