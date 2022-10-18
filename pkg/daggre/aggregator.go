package daggre

import (
	"errors"
	"fmt"
	"log"
)

// ============================== Aggregator start ==============================

type Aggregator struct {
	Pipelines []*Pipeline `json:"pipelines" binding:"required"`
	Main      string      `json:"main" binding:"required"`

	// data source dataset
	data *Data
	// cache result cache for pipelines
	cache map[string]*Table
}

func (a *Aggregator) GetPipeline(name string) *Pipeline {
	for _, p := range a.Pipelines {
		if p.Name == name {
			return p
		}
	}
	return nil
}

func (a *Aggregator) GetPipelineData(name string) (*Table, error) {
	cached, ok := a.cache[name]
	if ok {
		return cached, nil
	}

	pipeline := a.GetPipeline(name)
	if pipeline == nil {
		return nil, errors.New(fmt.Sprintf("no pipeline named %s", name))
	}

	return pipeline.Process(a)
}

func (a *Aggregator) Data() *Data {
	return a.data
}

func (a *Aggregator) Reset() {
	a.data = nil
	a.cache = make(map[string]*Table)
}

func (a *Aggregator) Aggregate(data *Data) (*Table, error) {
	a.Reset()
	a.data = data
	return a.GetPipelineData(a.Main)
}

// ============================== Aggregator end ==============================

type PipelineStageInterface interface {
	Process(*Table, *Aggregator) *Table
}

var PipelineStageFactory = map[string]func(PipelineStageParams) PipelineStageInterface{
	"filter": NewFilterStage,
	"lookup": NewLookupStage,
	"sort":   NewSortStage,
	"unwind": NewUnwindStage,
}

func (p *Pipeline) Process(a *Aggregator) (*Table, error) {
	tb := a.Data().GetMergedTables(p.Tables...)

	for _, stage := range p.Stages {
		stageInterfaceFactory, ok := PipelineStageFactory[stage.Name]
		if !ok {
			// TODO: graceful implementations
			log.Panicf("unsupported stage %s\n", stage.Name)
		}
		stageInterface := stageInterfaceFactory(stage.Params)
		tb = stageInterface.Process(tb, a)
		if len(*tb) == 0 {
			log.Printf("empty data after stage %+v\n", stage)
			break
		}
	}

	return tb, nil
}
