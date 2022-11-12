package daggre

import (
	"errors"
	"fmt"
	"log"
	"time"
)

type Aggregator struct {
	Pipelines []*Pipeline `json:"pipelines" binding:"required"`
	Main      string      `json:"main" binding:"required"`

	// data source dataset
	data *Data
	// cache result cache for pipelines
	cache map[string]*PipelineResult
	// running flag to indicate if aggregator is running
	running bool
}

func (a *Aggregator) Pipeline(name string) *Pipeline {
	for _, p := range a.Pipelines {
		if p.Name == name {
			return p
		}
	}
	return nil
}

func (a *Aggregator) MainPipeline() *Pipeline {
	return a.Pipeline(a.Main)
}

func (a *Aggregator) Reset(data *Data) {
	a.data = data
	a.cache = make(map[string]*PipelineResult)
}

func (a *Aggregator) inputTable(p *Pipeline) *Table {
	if a.data == nil || p == nil {
		return &Table{}
	}
	return a.data.GetMergedTables(p.Tables...)
}

func logStage(pipelineName string, stageNum int, stageName string, msg string) {
	log.Printf("[%s][Stage %d][%s]: %s", pipelineName, stageNum, stageName, msg)
}

func makeStageErr(pipelineName string, stageNum int, stageName string, errMsg string) error {
	return fmt.Errorf("[%s][Stage %d][%s]: %s", pipelineName, stageNum, stageName, errMsg)
}

func (a *Aggregator) process(p *Pipeline) *PipelineResult {
	ret := NewPipelineResult()
	if p == nil {
		ret.err = fmt.Errorf("nil pipeline")
		return ret
	}

	defer func() {
		a.cache[p.Name] = ret
	}()

	pipelineName := p.Name

	// merge all input tables
	tb := a.inputTable(p)
	ret.inputSize = len(*tb)

	// process all stages
	for i, stage := range p.Stages {
		stageNum := i + 1
		stageName := stage.Name
		stageRet := NewPipelineStageResult()

		if !ret.Success() {
			logStage(pipelineName, stageNum, stageName, "skipped due to failure")
			stageRet.SetProcResult(nil)
			ret.AppendStageResult(stageRet)
			tb = &Table{}
			continue
		}

		var stageProcRet *PipelineStageProcResult
		stageInterfaceFactory, ok := PipelineStageFactoryMap[stageName]
		if !ok {
			errMsg := fmt.Sprintf("unsupported stage %s", stageName)
			stageProcRet = &PipelineStageProcResult{
				tb:  &Table{},
				err: errors.New(errMsg),
			}
			stageRet.SetProcResult(stageProcRet)
			ret.AppendStageResult(stageRet)
			logStage(pipelineName, stageNum, stageName, fmt.Sprintf("error occured, %s", errMsg))
			ret.err = makeStageErr(p.Name, stageNum, stageName, errMsg)
			tb = stageProcRet.tb
			continue
		}

		logStage(pipelineName, stageNum, stageName, "start processing...")
		stageInterface := stageInterfaceFactory(stage.Params)
		stageProcRet = stageInterface.Process(tb, a)
		stageRet.SetProcResult(stageProcRet)
		if stageRet.Success() {
			logStage(pipelineName, stageNum, stageName, "process successfully")
		} else if stageRet.Fail() {
			logStage(pipelineName, stageNum, stageName, "process failed: "+stageRet.err.Error())
			ret.err = makeStageErr(p.Name, stageNum, stageName, stageRet.err.Error())
		} else {
			logStage(pipelineName, stageNum, stageName, "process unexpectedly skipped!!!")
			ret.err = makeStageErr(p.Name, stageNum, stageName, "unexpectedly skipped")
		}
		ret.AppendStageResult(stageRet)
		tb = stageProcRet.tb
	}
	ret.output = tb
	ret.endTime = time.Now()
	return ret
}

// RuntimePipelineResult returns the pipeline result in an already-started aggregation session
func (a *Aggregator) RuntimePipelineResult(name string) *PipelineResult {
	cached, ok := a.cache[name]
	if ok {
		return cached
	}

	// do not process pipeline if not running
	if !a.running {
		return nil
	}

	var ret *PipelineResult
	pipeline := a.Pipeline(name)
	if pipeline == nil {
		ret = NewPipelineResult()
		ret.err = fmt.Errorf("pipeline %s not found", name)
	} else {
		ret = a.process(pipeline)
	}
	return ret
}

type AggreResult struct {
	pipelineResult *PipelineResult
	Output         *Table                   `json:"output"`
	Stats          map[string]*PipelineStat `json:"stats"`
}

func (a *Aggregator) checkPipeline(name string, visited map[string]bool) error {
	_, ok := visited[name]
	if ok {
		return fmt.Errorf("[%s] detected circular reference", name)
	}
	visited[name] = true
	defer delete(visited, name)

	pipeline := a.Pipeline(name)
	if pipeline == nil {
		return fmt.Errorf("[%s] pipeline not found", name)
	}

	for i, stage := range pipeline.Stages {
		stageNum := i + 1
		stageName := stage.Name
		stageInterfaceFactory, ok := PipelineStageFactoryMap[stageName]
		if !ok {
			return fmt.Errorf("[%s][%d][%s] unsupported stage", name, stageNum, stageName)
		}
		stageInterface := stageInterfaceFactory(stage.Params)
		stageInterfaceErr := stageInterface.Check()
		if stageInterfaceErr != nil {
			return fmt.Errorf("[%s][%d][%s] check failed, %v", name, stageNum, stageName, stageInterfaceErr)
		}
		referencedPipelines := stageInterface.ChildPipelines()
		for _, pipelineName := range referencedPipelines {
			err := a.checkPipeline(pipelineName, visited)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (a *Aggregator) CheckPipelines() error {
	visited := make(map[string]bool)
	return a.checkPipeline(a.Main, visited)
}

func (a *Aggregator) Aggregate(data *Data) *AggreResult {
	a.Reset(data)
	ret := &AggreResult{
		Output:         nil,
		Stats:          make(map[string]*PipelineStat),
		pipelineResult: nil,
	}
	var pipelineResult *PipelineResult

	// check validity of all pipeline stages
	err := a.CheckPipelines()
	if err != nil {
		pipelineResult = NewPipelineResult()
		pipelineResult.err = err
	} else {
		a.running = true
		pipelineResult = a.process(a.MainPipeline())
		a.running = false
		if pipelineResult != nil {
			ret.Output = pipelineResult.Output()
		}
		for name, cachedResult := range a.cache {
			stat := cachedResult.Stat()
			if name != "" && stat != nil {
				ret.Stats[name] = stat
			}
		}
	}
	return ret
}
