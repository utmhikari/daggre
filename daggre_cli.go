package main

import (
	"github.com/urfave/cli"
	cliModule "github.com/utmhikari/daggre/internal/cli"
	"log"
	"os"
)

func main() {
	app := cli.NewApp()
	app.Name = "daggre-cli"
	app.Usage = "DAta-AGGREgator client, a tool to handle aggregation on lists of dict-data"
	app.Version = "0.9.9"

	args := &cliModule.Args{}

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:        "workdir",
			Usage:       "CLI: working directory of source files (optional)",
			Value:       "",
			Destination: &args.WorkDir,
		},
		cli.StringFlag{
			Name:        "datapath",
			Usage:       "CLI: data source file path",
			Value:       "data.json",
			Destination: &args.DataPath,
		},
		cli.StringFlag{
			Name:        "aggrepath",
			Usage:       "CLI: aggregator file path",
			Value:       "aggre.json",
			Destination: &args.AggrePath,
		},
		cli.StringFlag{
			Name:        "outputpath",
			Usage:       "CLI: data aggregation output file path",
			Value:       "output.json",
			Destination: &args.OutputPath,
		},
	}

	app.Action = func(c *cli.Context) {
		log.Println("start daggre-cli...")
		cliModule.Start(args)
	}

	if err := app.Run(os.Args); err != nil {
		log.Panic(err)
	}
}
