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
			Name:        "dir,d",
			Usage:       "CLI: root directory which contains data & aggregator",
			Destination: &args.Dir,
		},
		cli.StringFlag{
			Name:        "datafile",
			Usage:       "CLI: data source file name",
			Value:       "data.json",
			Destination: &args.DataFile,
		},
		cli.StringFlag{
			Name:        "aggrefile",
			Usage:       "CLI: aggregator file name",
			Value:       "aggre.json",
			Destination: &args.AggreFile,
		},
		cli.StringFlag{
			Name:        "outputfile",
			Usage:       "CLI: data aggregation output file name",
			Value:       "output.json",
			Destination: &args.OutputFile,
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
