package daggre

import (
	"errors"
	"fmt"
	"log"
	"time"
)

type Pipeline struct {
	Name   string           `json:"name"`
	Desc   string           `json:"desc"`
	Tables []string         `json:"tables"`
	Stages []*PipelineStage `json:"stages"`
}

type PipelineResult struct {
	output    *Table
	err       error
	inputSize int
	results   []*PipelineStageResult
	startTime time.Time
	endTime   time.Time
}

func (r *PipelineResult) Success() bool {
	return r.err == nil
}

func (r *PipelineResult) Error() string {
	if r.err == nil {
		return ""
	}
	return r.err.Error()
}

func (r *PipelineResult) Output() *Table {
	return r.output
}

func (r *PipelineResult) AppendStageResult(result *PipelineStageResult) {
	r.results = append(r.results, result)
}

type PipelineStat struct {
	Success    bool                 `json:"success"`
	Error      string               `json:"error"`
	InputSize  int                  `json:"inputSize"`
	OutputSize int                  `json:"outputSize"`
	StartTime  int64                `json:"startTime"`
	EndTime    int64                `json:"endTime"`
	StageStats []*PipelineStageStat `json:"stageStats"`
}

func (r *PipelineResult) Stat() *PipelineStat {
	var errStr string
	if r.err != nil {
		errStr = r.err.Error()
	}
	var outputSize int
	if r.output != nil {
		outputSize = len(*(r.output))
	}
	stageStats := make([]*PipelineStageStat, len(r.results))
	for i, _ := range r.results {
		stageStats[i] = r.results[i].Stat()
	}
	return &PipelineStat{
		Success:    r.Success(),
		Error:      errStr,
		InputSize:  r.inputSize,
		OutputSize: outputSize,
		StartTime:  r.startTime.UnixMilli(),
		EndTime:    r.endTime.UnixMilli(),
		StageStats: stageStats,
	}
}

func NewPipelineResult(input *Table) *PipelineResult {
	var inputSize int
	if input != nil {
		inputSize = len(*input)
	}
	return &PipelineResult{
		output:    &Table{},
		err:       nil,
		inputSize: inputSize,
		results:   []*PipelineStageResult{},
		startTime: time.Now(),
		endTime:   time.Now(),
	}
}

func logStage(stageNum int, stageName string, msg string) {
	log.Printf("[Stage %d][%s]: %s", stageNum, stageName, msg)
}

func makeStageErr(pipelineName string, stageNum int, stageName string, errMsg string) error {
	return errors.New(fmt.Sprintf("[%s][Stage %d][%s]: %s", pipelineName, stageNum, stageName, errMsg))
}

func (p *Pipeline) Process(a *Aggregator) *PipelineResult {
	tb := a.Data().GetMergedTables(p.Tables...)
	ret := NewPipelineResult(tb)
	for i, stage := range p.Stages {
		stageNum := i + 1
		stageName := stage.Name
		stageRet := NewPipelineStageResult()

		if !ret.Success() {
			logStage(stageNum, stageName, "skipped due to failure")
			stageRet.SetProcResult(nil)
			ret.AppendStageResult(stageRet)
			tb = &Table{}
			continue
		}

		var stageProcRet *PipelineStageProcResult
		stageInterfaceFactory, ok := PipelineStageFactory[stageName]
		if !ok {
			errMsg := fmt.Sprintf("unsupported stage %s", stageName)
			stageProcRet = &PipelineStageProcResult{
				tb:  &Table{},
				err: errors.New(errMsg),
			}
			stageRet.SetProcResult(stageProcRet)
			ret.AppendStageResult(stageRet)
			logStage(stageNum, stageName, fmt.Sprintf("error occured, %s", errMsg))
			ret.err = makeStageErr(p.Name, stageNum, stageName, errMsg)
			tb = stageProcRet.tb
			continue
		}

		logStage(stageNum, stageName, "start processing...")
		stageInterface := stageInterfaceFactory(stage.Params)
		stageProcRet = stageInterface.Process(tb, a)
		stageRet.SetProcResult(stageProcRet)
		if stageRet.Success() {
			logStage(stageNum, stageName, "process successfully")
		} else if stageRet.Fail() {
			logStage(stageNum, stageName, "process failed: "+stageRet.err.Error())
			ret.err = makeStageErr(p.Name, stageNum, stageName, stageRet.err.Error())
		} else {
			logStage(stageNum, stageName, "process unexpectedly skipped!!!")
			ret.err = makeStageErr(p.Name, stageNum, stageName, "unexpectedly skipped")
		}
		ret.AppendStageResult(stageRet)
		tb = stageProcRet.tb
	}
	ret.output = tb
	ret.endTime = time.Now()
	return ret
}
