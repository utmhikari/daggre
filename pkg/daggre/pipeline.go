package daggre

import (
	"time"
)

type Pipeline struct {
	Name   string           `json:"name"`
	Desc   string           `json:"desc"`
	Tables []string         `json:"tables"`
	Stages []*PipelineStage `json:"stages"`
}

type PipelineResult struct {
	// TODO: memory leak? currently assuming that one pipeline would be used by multiple parents
	// TODO: use child pipeline reference count to check if output table is useless?
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

func NewPipelineResult() *PipelineResult {
	return &PipelineResult{
		output:    &Table{},
		err:       nil,
		inputSize: 0,
		results:   []*PipelineStageResult{},
		startTime: time.Now(),
		endTime:   time.Now(),
	}
}
