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

	ret := pipeline.Process(a)
	return ret.OutputTable, nil
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
