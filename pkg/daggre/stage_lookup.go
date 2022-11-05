package daggre

import (
	"fmt"
	"github.com/utmhikari/daggre/pkg/util"
	"log"
)

type LookupStage struct {
	BasePipelineStage
	FromPipeline   string `json:"fromPipeline"`
	LocalLocator   string `json:"localLocator"`
	ForeignLocator string `json:"foreignLocator"`
	ToField        string `json:"toField"`
}

func (l *LookupStage) Check() error {
	if l.FromPipeline == "" {
		return fmt.Errorf("fromPipeline must be specified")
	}
	localLocator := NewLocator(l.LocalLocator)
	if !localLocator.Valid() {
		return fmt.Errorf("invalid localLocator: %s", l.LocalLocator)
	}
	foreignLocator := NewLocator(l.ForeignLocator)
	if !foreignLocator.Valid() {
		return fmt.Errorf("invalid foreignLocator: %s", l.ForeignLocator)
	}
	if l.ToField == "" {
		return fmt.Errorf("toField must be specified")
	}
	return nil
}

func (l *LookupStage) ChildPipelines() []string {
	if l.FromPipeline != "" {
		return []string{l.FromPipeline}
	}
	return []string{}
}

func (l *LookupStage) Process(tb *Table, a *Aggregator) *PipelineStageProcResult {
	log.Printf("lookup stage: %s\n", util.JsonDump(l))

	ret := &PipelineStageProcResult{
		tb:  &Table{},
		err: nil,
	}

	fromPipelineResult := a.RuntimePipelineResult(l.FromPipeline)
	if fromPipelineResult.err != nil {
		ret.err = fmt.Errorf("fromPipeline error => %s", fromPipelineResult.Error())
		return ret
	}

	foreignLocator := NewLocator(l.ForeignLocator)
	fromTbRowMap := make(map[interface{}][]*Row)
	fromTb := fromPipelineResult.Output()
	for _, row := range *fromTb {
		locatedValue := foreignLocator.Locate(row)
		if locatedValue != nil {
			rows, ok := fromTbRowMap[locatedValue]
			if !ok {
				fromTbRowMap[locatedValue] = []*Row{row}
			} else {
				fromTbRowMap[locatedValue] = append(rows, row)
			}
		}
	}

	localLocator := NewLocator(l.LocalLocator)
	for _, row := range *tb {
		locatedValue := localLocator.Locate(row)
		rows, ok := fromTbRowMap[locatedValue]
		if ok {
			var newRows []*Row
			for _, r := range rows {
				newRows = append(newRows, r.Copy())
			}
			(*row)[l.ToField] = newRows
		}
	}

	ret.tb = tb
	return ret
}

func NewLookupStage(params PipelineStageParams) PipelineStageInterface {
	lookupStage := LookupStage{}
	lookupStage.FromPipeline = util.ToString(params["fromPipeline"])
	lookupStage.LocalLocator = util.ToString(params["localLocator"])
	lookupStage.ForeignLocator = util.ToString(params["foreignLocator"])
	lookupStage.ToField = util.ToString(params["toField"])
	return &lookupStage
}
