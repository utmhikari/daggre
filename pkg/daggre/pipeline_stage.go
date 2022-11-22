package daggre

import (
	"errors"
	"time"
)

type PipelineStageParams map[string]interface{}

type PipelineStage struct {
	Name   string              `json:"name"`
	Desc   string              `json:"desc"`
	Params PipelineStageParams `json:"params"`
}

const (
	PipelineStageStatusSkip    = 0
	PipelineStageStatusSuccess = 1
	PipelineStageStatusFail    = -1
)

type PipelineStageResult struct {
	status    int
	err       error
	startTime time.Time
	endTime   time.Time
	output    *Table
}

func (r *PipelineStageResult) Skip() bool {
	return r.status == PipelineStageStatusSkip
}

func (r *PipelineStageResult) Success() bool {
	return r.status == PipelineStageStatusSuccess
}

func (r *PipelineStageResult) Fail() bool {
	return r.status == PipelineStageStatusFail
}

type PipelineStageStat struct {
	Status     string `json:"status"`
	Error      string `json:"error"`
	StartTime  int64  `json:"startTime"`
	EndTime    int64  `json:"endTime"`
	OutputSize int    `json:"outputSize"`
}

func (r *PipelineStageResult) Stat() *PipelineStageStat {
	statusStr := "unknown"
	switch r.status {
	case PipelineStageStatusSkip:
		statusStr = "skip"
		break
	case PipelineStageStatusFail:
		statusStr = "fail"
		break
	case PipelineStageStatusSuccess:
		statusStr = "success"
		break
	default:
		break
	}

	errStr := ""
	if r.err != nil {
		errStr = r.err.Error()
	}

	length := 0
	if r.output != nil {
		length = len(*(r.output))
	}

	return &PipelineStageStat{
		Status:     statusStr,
		Error:      errStr,
		StartTime:  r.startTime.UnixMilli(),
		EndTime:    r.endTime.UnixMilli(),
		OutputSize: length,
	}
}

func NewPipelineStageResult() *PipelineStageResult {
	return &PipelineStageResult{
		status:    PipelineStageStatusSkip,
		err:       nil,
		startTime: time.Now(),
		endTime:   time.Now(),
		output:    &Table{},
	}
}

type PipelineStageProcResult struct {
	tb  *Table
	err error
}

func (pspr *PipelineStageProcResult) GetTable() *Table {
	return pspr.tb
}

func (pspr *PipelineStageProcResult) SetError(err error) {
	pspr.err = err
}

func NewPipelineStageProcResult() *PipelineStageProcResult {
	return &PipelineStageProcResult{
		tb:  &Table{},
		err: nil,
	}
}

func (r *PipelineStageResult) SetProcResult(procResult *PipelineStageProcResult) {
	r.endTime = time.Now()

	// no process result given, meaning that the stage process has been skipped
	if procResult == nil {
		r.err = errors.New("stage is skipped")
	}

	// check process result
	r.output = procResult.tb
	if r.output == nil || len(*r.output) == 0 {
		r.status = PipelineStageStatusFail
		if procResult.err == nil {
			r.err = errors.New("output table is empty")
		} else {
			r.err = procResult.err
		}
	} else {
		if procResult.err == nil {
			r.status = PipelineStageStatusSuccess
		} else {
			r.status = PipelineStageStatusFail
			r.err = procResult.err
		}
	}
}

type PipelineStageInterface interface {
	Check() error
	ChildPipelines() []string
	Process(*Table, *Aggregator) *PipelineStageProcResult
}

type BasePipelineStage struct{}

func (bps *BasePipelineStage) Check() error {
	return nil
}

func (bps *BasePipelineStage) ChildPipelines() []string {
	return []string{}
}

func (bps *BasePipelineStage) Process(tb *Table, a *Aggregator) *PipelineStageProcResult {
	return &PipelineStageProcResult{
		tb:  tb,
		err: nil,
	}
}

type PipelineStageFactory func(PipelineStageParams) PipelineStageInterface

var PipelineStageFactoryMap = map[string]PipelineStageFactory{
	"filter": NewFilterStage,
	"lookup": NewLookupStage,
	"sort":   NewSortStage,
	"unwind": NewUnwindStage,
}

func RegisterPipelineStage(name string, factory PipelineStageFactory) {
	PipelineStageFactoryMap[name] = factory
}

func UnregisterPipelineStage(name string) {
	_, ok := PipelineStageFactoryMap[name]
	if ok {
		delete(PipelineStageFactoryMap, name)
	}
}
