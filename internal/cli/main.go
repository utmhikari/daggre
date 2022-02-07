package cli

import (
	"github.com/utmhikari/daggre/internal/cmd"
	"github.com/utmhikari/daggre/pkg/daggre"
	"github.com/utmhikari/daggre/pkg/util"
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
	rule := &daggre.PipelineRule{}
	rulePath := path.Join(cmd.CliParams.Dir, cmd.CliParams.RuleFile)
	err = util.ReadJsonFile(rulePath, &rule)
	if err != nil {
		log.Panicf("failed to load rule file, %s\n", err.Error())
	}

	log.Printf("data: %+v\n", data)
	log.Printf("rule: %+v\n", rule)

	tb, err := rule.Apply(data)
	if err != nil {
		log.Panicf("failed to run daggre, %s\n", err.Error())
	}
	log.Printf("output: %+v\n", tb)
}
