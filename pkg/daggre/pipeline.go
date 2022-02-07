package daggre

import (
	"errors"
	"log"
)

type PipelineStageParams map[string]interface{}

type PipelineStage struct {
	Name   string              `json:"name"`
	Params PipelineStageParams `json:"params"`
}

type Pipeline struct {
	Name   string           `json:"name"`
	Desc   string           `json:"desc"`
	Tables []string         `json:"tables"`
	Stages []*PipelineStage `json:"stages"`
}

type PipelineRule struct {
	Pipelines []*Pipeline `json:"pipelines"`
	Main      string      `json:"main"`

	data *Data
}

type PipelineStageInterface interface {
	Process(*Table, *PipelineRule) *Table
}

var PipelineStageFactory = map[string]func(PipelineStageParams) PipelineStageInterface{
	"filter": NewFilterStage,
}

func (p *Pipeline) Process(r *PipelineRule) (*Table, error) {
	tb := r.data.GetMergedTables(p.Tables...)

	for _, stage := range p.Stages {
		stageInterfaceFactory, ok := PipelineStageFactory[stage.Name]
		if !ok {
			log.Fatalf("unsupported stage %s\n", stage.Name)
		}
		stageInterface := stageInterfaceFactory(stage.Params)
		tb = stageInterface.Process(tb, r)
	}
	
	return tb, nil
}

func (r *PipelineRule) GetPipeline(name string) *Pipeline {
	for _, p := range r.Pipelines {
		if p.Name == name {
			return p
		}
	}
	return nil
}

func (r *PipelineRule) MainPipeline() *Pipeline {
	return r.GetPipeline(r.Main)
}

func (r *PipelineRule) Apply(data *Data) (*Table, error) {
	r.data = data
	mainPipeline := r.MainPipeline()
	if mainPipeline == nil {
		return nil, errors.New("no main pipeline found")
	}
	return mainPipeline.Process(r)
}
