package daggre

import (
	"errors"
	"fmt"
	"log"
	"time"
)

type PipelineStageParams map[string]interface{}

type PipelineStage struct {
	Name   string              `json:"name"`
	Desc   string              `json:"desc"`
	Params PipelineStageParams `json:"params"`
}

type Pipeline struct {
	Name   string           `json:"name"`
	Desc   string           `json:"desc"`
	Tables []string         `json:"tables"`
	Stages []*PipelineStage `json:"stages"`
}

type PipelineStageProcessResult struct {
	Table  *Table
	Err    error
	Detail map[string]interface{}
}

const (
	PipelineStageStatusSkip    = 0
	PipelineStageStatusSuccess = 1
	PipelineStageStatusFail    = -1
)

type PipelineStageResult struct {
	Status     int                    `json:"status"`
	Message    string                 `json:"message"`
	Detail     map[string]interface{} `json:"detail"`
	StartTime  int64                  `json:"startTime"`
	EndTime    int64                  `json:"endTime"`
	PrevLength int                    `json:"prevLength"`
	CurLength  int                    `json:"curLength"`

	prevTb *Table
	curTb  *Table
}

func (r *PipelineStageResult) Success() bool {
	return r.Status == PipelineStageStatusSuccess
}

func (r *PipelineStageResult) Fail() bool {
	return r.Status == PipelineStageStatusFail
}

func NewPipelineStageResult(prevTb *Table, startTime int64, procResult *PipelineStageProcessResult) *PipelineStageResult {
	ret := &PipelineStageResult{
		Status:     PipelineStageStatusSkip,
		Message:    "",
		Detail:     nil,
		PrevLength: 0,
		CurLength:  0,
		StartTime:  startTime,
		EndTime:    time.Now().UnixMilli(),

		prevTb: prevTb,
		curTb:  nil,
	}

	// fix time duration
	if ret.StartTime > ret.EndTime || ret.StartTime <= 0 {
		ret.StartTime = ret.EndTime
	}

	// no prev table given, meaning that the stage process has never been launched
	if ret.prevTb == nil {
		return ret
	}
	ret.PrevLength = len(*ret.prevTb)

	// no process result given, meaning that the stage process has been skipped
	if procResult == nil {
		ret.Message = "process is skipped"
		return ret
	}

	// check process result
	ret.curTb = procResult.Table
	ret.Detail = procResult.Detail
	if ret.curTb == nil || len(*ret.curTb) == 0 {
		ret.Status = PipelineStageStatusFail
		if procResult.Err == nil {
			ret.Message = "processed table is empty"
		} else {
			ret.Message = procResult.Err.Error()
		}
	} else {
		ret.CurLength = len(*ret.curTb)
		if procResult.Err == nil {
			ret.Status = PipelineStageStatusSuccess
		} else {
			ret.Status = PipelineStageStatusFail
			ret.Message = procResult.Err.Error()
		}
	}
	return ret
}

type PipelineStageInterface interface {
	Process(*Table, *Aggregator) *PipelineStageProcessResult
}

var PipelineStageFactory = map[string]func(PipelineStageParams) PipelineStageInterface{
	"filter": NewFilterStage,
	"lookup": NewLookupStage,
	"sort":   NewSortStage,
	"unwind": NewUnwindStage,
}

type PipelineProcessResult struct {
	OutputTable  *Table                 `json:"outputTable"`
	StageResults []*PipelineStageResult `json:"stageResults"`
	Success      bool                   `json:"success"`
	Message      string                 `json:"message"`
}

func (p *Pipeline) Process(a *Aggregator) *PipelineProcessResult {
	tb := a.Data().GetMergedTables(p.Tables...)
	ret := &PipelineProcessResult{
		OutputTable:  &Table{},
		StageResults: []*PipelineStageResult{},
		Success:      true,
		Message:      "",
	}

	for i, stage := range p.Stages {
		stageNum := i + 1

		if !ret.Success {
			log.Printf("Stage %d (%s) -> skipped due to failure", stageNum, stage.Name)
			ret.StageResults = append(ret.StageResults, NewPipelineStageResult(tb, 0, nil))
			tb = &Table{}
			continue
		}

		stageInterfaceFactory, ok := PipelineStageFactory[stage.Name]
		if !ok {
			errMsg := fmt.Sprintf("unsupported stage %s", stage.Name)
			stageProcRet := &PipelineStageProcessResult{
				Table:  &Table{},
				Err:    errors.New(errMsg),
				Detail: nil,
			}
			ret.StageResults = append(ret.StageResults, NewPipelineStageResult(tb, 0, stageProcRet))
			log.Printf("Stage %d (%s) -> %s", stageNum, stage.Name, errMsg)
			ret.Success = false
			ret.Message = fmt.Sprintf("#%d -> %s", stageNum, errMsg)
			continue
		}

		log.Printf("Stage %d (%s) -> start processing...", stageNum, stage.Name)
		startTime := time.Now().UnixMilli()
		stageInterface := stageInterfaceFactory(stage.Params)
		procResult := stageInterface.Process(tb, a)
		stageRet := NewPipelineStageResult(tb, startTime, procResult)
		if stageRet.Success() {

		} else if stageRet.Fail() {
			ret.Success = false

		} else {
			ret.Success = false
			ret.Message = fmt.Sprintf("#%d -> unexpectedly skipped...", stageNum)
		}
		ret.StageResults = append(ret.StageResults, stageRet)
		tb = procResult.Table
	}

	ret.OutputTable = tb
	return ret
}
