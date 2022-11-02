package daggre

import (
	"errors"
	"fmt"
)

type Aggregator struct {
	Pipelines []*Pipeline `json:"pipelines" binding:"required"`
	Main      string      `json:"main" binding:"required"`

	// data source dataset
	data *Data
	// cache result cache for pipelines
	cache map[string]*PipelineResult
}

func (a *Aggregator) Pipeline(name string) *Pipeline {
	for _, p := range a.Pipelines {
		if p.Name == name {
			return p
		}
	}
	return nil
}

func (a *Aggregator) PipelineResult(name string) *PipelineResult {
	cached, ok := a.cache[name]
	if ok {
		return cached
	}

	pipeline := a.Pipeline(name)
	if pipeline == nil {
		ret := NewPipelineResult(nil)
		ret.err = errors.New(fmt.Sprintf("no pipeline named %s", name))
		return ret
	}

	return pipeline.Process(a)
}

func (a *Aggregator) Data() *Data {
	return a.data
}

func (a *Aggregator) Reset() {
	a.data = nil
	a.cache = make(map[string]*PipelineResult)
}

type AggregateResult struct {
	Output *Table        `json:"output"`
	Stat   *PipelineStat `json:"stat"`
}

func (a *Aggregator) Aggregate(data *Data) *AggregateResult {
	a.Reset()
	a.data = data
	ret := a.PipelineResult(a.Main)
	return &AggregateResult{
		Output: ret.Output(),
		Stat:   ret.Stat(),
	}
}
