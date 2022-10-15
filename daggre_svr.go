package main

import (
	"github.com/urfave/cli"
	svrModule "github.com/utmhikari/daggre/internal/svr"
	"log"
	"os"
)

func main() {
	app := cli.NewApp()
	app.Name = "daggre-svr"
	app.Usage = "DAta-AGGREgator server, a tool to handle aggregation on lists of dict-data"
	app.Version = "0.9.9"

	args := &svrModule.Args{}

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "cfgpath,c",
			Usage:       "SVR: config file path (in yaml format)",
			Value:       "cfg/svr.yaml",
			Destination: &args.CfgPath,
		},
	}

	app.Action = func(c *cli.Context) {
		svrModule.Start(args)
	}

	if err := app.Run(os.Args); err != nil {
		log.Panic(err)
	}
}
