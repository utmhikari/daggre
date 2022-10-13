package main

import (
	"github.com/urfave/cli"
	cliHandler "github.com/utmhikari/daggre/internal/cli"
	"github.com/utmhikari/daggre/internal/cmd"
	"github.com/utmhikari/daggre/internal/svr"
	"log"
	"os"
)

func appAction(c *cli.Context) {
	log.Printf("app params: %+v\n", cmd.AppParams)

	mode := c.String("mode")
	switch mode {
	case cmd.ModeCli:
		cliHandler.Start()
	case cmd.ModeSvr:
		svr.Start()
	default:
		log.Panicf("invalid app mode %s", mode)
	}
}

func main() {
	app := cli.NewApp()
	app.Name = "daggre"
	app.Usage = "DAta-AGGREgator, a tool to handle aggregation on lists of dict-data"
	app.Version = "0.9.9"

	app.Flags = []cli.Flag{
		// =================== app start ===================
		&cli.StringFlag{
			Name:        "mode,m",
			Usage:       "APP: launch mode -> cli, svr",
			Value:       "cli",
			Destination: &cmd.AppParams.Mode,
		},
		// =================== app end ===================

		// =================== cli start ===================
		cli.StringFlag{
			Name:        "dir,d",
			Usage:       "CLI: root directory which contains data & aggregator",
			Destination: &cmd.CliParams.Dir,
		},
		cli.StringFlag{
			Name:        "datafile",
			Usage:       "CLI: data source file name",
			Value:       "data.json",
			Destination: &cmd.CliParams.DataFile,
		},
		cli.StringFlag{
			Name:        "aggrefile",
			Usage:       "CLI: aggregator file name",
			Value:       "aggre.json",
			Destination: &cmd.CliParams.AggreFile,
		},
		cli.StringFlag{
			Name:        "outputfile",
			Usage:       "CLI: data aggregation output file name",
			Value:       "output.json",
			Destination: &cmd.CliParams.OutputFile,
		},
		// =================== cli end ===================

		// =================== svr start ===================
		cli.StringFlag{
			Name:        "cfgpath,c",
			Usage:       "SVR: config file path",
			Value:       "cfg/svr.json",
			Destination: &cmd.SvrParams.CfgPath,
		},
		// =================== svr end ===================
	}

	app.Action = appAction

	if err := app.Run(os.Args); err != nil {
		log.Panic(err)
	}
}
