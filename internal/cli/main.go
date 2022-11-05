package cli

import (
	"github.com/utmhikari/daggre/pkg/daggre"
	"github.com/utmhikari/daggre/pkg/util"
	"io/ioutil"
	"log"
	"path"
)

func Start(args *Args) {
	log.Printf("cli cmd args: %+v\n", args)

	// load data
	data := &daggre.Data{}
	dataPath := path.Join(args.WorkDir, args.DataPath)
	err := util.ReadJsonFile(dataPath, &data)
	if err != nil {
		log.Panicf("failed to load data file, %s\n", err.Error())
	}

	// load pipeline
	aggregator := &daggre.Aggregator{}
	aggregatorPath := path.Join(args.WorkDir, args.AggrePath)
	err = util.ReadJsonFile(aggregatorPath, &aggregator)
	if err != nil {
		log.Panicf("failed to load aggregator file, %s\n", err.Error())
	}

	log.Printf("data: %s\n", util.JsonDump(data))
	log.Printf("aggregator: %s\n", util.JsonDump(aggregator))

	aggreResult := aggregator.Aggregate(data)

	descendantStatStr := util.JsonDump(aggreResult.DescendantStats)
	log.Printf("descendantStat: %s\n", descendantStatStr)

	statStr := util.JsonDump(aggreResult.Stat)
	log.Printf("stat: %s\n", statStr)

	outputStr := util.JsonDump(aggreResult.Output)
	log.Printf("output: %s\n", outputStr)
	outputPath := path.Join(args.WorkDir, args.OutputPath)
	err = ioutil.WriteFile(outputPath, []byte(outputStr), 0644)
	if err != nil {
		log.Panicf("failed to dump result to output file, %s\n", err.Error())
	}
	log.Printf("dump result successfully")
}
