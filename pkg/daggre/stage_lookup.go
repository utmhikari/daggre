package daggre

import (
	"errors"
	"fmt"
	"github.com/utmhikari/daggre/pkg/util"
	"log"
)

type LookupStage struct {
	FromPipeline   string `json:"fromPipeline"`
	LocalLocator   string `json:"localLocator"`
	ForeignLocator string `json:"foreignLocator"`
	ToField        string `json:"toField"`
}

func (l *LookupStage) Process(tb *Table, a *Aggregator) *PipelineStageProcessResult {
	log.Printf("lookup stage: %s\n", util.JsonDump(l))

	ret := &PipelineStageProcessResult{
		Table:  &Table{},
		Err:    nil,
		Detail: nil,
	}

	if len(l.ToField) == 0 {
		ret.Err = errors.New("param 'ToField' is required")
		return ret
	}

	fromTb, err := a.GetPipelineData(l.FromPipeline)
	if err != nil {
		ret.Err = errors.New(fmt.Sprintf("err at pipeline %s -> %s", l.FromPipeline, err.Error()))
		return ret
	}

	foreignLocator := NewLocator(l.ForeignLocator)
	fromTbRowMap := make(map[interface{}][]*Row)
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

	ret.Table = tb
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
