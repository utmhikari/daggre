package svr

import (
	"github.com/utmhikari/daggre/internal/cmd"
	"log"
)

func Start() {
	log.Printf("svr params: %+v\n", cmd.SvrParams)
	log.Fatalln("svr mode not supported right now")
}
