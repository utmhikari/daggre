package cmd

const (
	ModeCli = "cli"
	ModeSvr = "svr"
)

type appParams struct {
	Mode string
}

var AppParams = &appParams{}
