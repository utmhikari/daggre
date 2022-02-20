package cli

import (
	"encoding/json"
	"github.com/utmhikari/daggre/internal/cmd"
	"github.com/utmhikari/daggre/pkg/daggre"
	"github.com/utmhikari/daggre/pkg/util"
	"io/ioutil"
	"log"
	"path"
)

func Start() {
	log.Printf("cli params: %+v\n", cmd.CliParams)

	// load data
	data := &daggre.Data{}
	dataPath := path.Join(cmd.CliParams.Dir, cmd.CliParams.DataFile)
	err := util.ReadJsonFile(dataPath, &data)
	if err != nil {
		log.Panicf("failed to load data file, %s\n", err.Error())
	}

	// load pipeline
	aggregator := &daggre.Aggregator{}
	aggregatorPath := path.Join(cmd.CliParams.Dir, cmd.CliParams.AggreFile)
	err = util.ReadJsonFile(aggregatorPath, &aggregator)
	if err != nil {
		log.Panicf("failed to load aggregator file, %s\n", err.Error())
	}

	log.Printf("data: %+v\n", data)
	log.Printf("aggregator: %+v\n", aggregator)

	tb, err := aggregator.Aggregate(data)
	if err != nil {
		log.Panicf("failed to process aggregator, %s\n", err.Error())
	}

	jsonData, err := json.MarshalIndent(tb, "", "  ")
	if err != nil {
		log.Panicf("failed to marshal result as json, %s\n", err.Error())
	}

	outputPath := path.Join(cmd.CliParams.Dir, cmd.CliParams.OutputFile)
	err = ioutil.WriteFile(outputPath, jsonData, 0644)
	if err != nil {
		log.Panicf("failed to dump result to output file, %s\n", err.Error())
	}
	log.Printf("dump result successfully")
}
