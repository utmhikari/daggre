package daggre

import (
	"github.com/utmhikari/daggre/pkg/util"
	"log"
)

type LookupStage struct {
	FromPipeline   string `json:"fromPipeline"`
	LocalLocator   string `json:"localLocator"`
	ForeignLocator string `json:"foreignLocator"`
	ToField        string `json:"toField"`
}

func (l *LookupStage) Process(tb *Table, a *Aggregator) *Table {
	log.Printf("lookup stage: %s\n", util.JsonDump(l))

	// TODO: validation
	if len(l.ToField) == 0 {
		return &Table{}
	}

	fromTb, err := a.GetPipelineData(l.FromPipeline)
	if err != nil {
		log.Printf("err at pipeline %s -> %s\n", l.FromPipeline, err.Error())
		return &Table{}
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

	return tb
}

func NewLookupStage(params PipelineStageParams) PipelineStageInterface {
	lookupStage := LookupStage{}
	lookupStage.FromPipeline = util.ToString(params["fromPipeline"])
	lookupStage.LocalLocator = util.ToString(params["localLocator"])
	lookupStage.ForeignLocator = util.ToString(params["foreignLocator"])
	lookupStage.ToField = util.ToString(params["toField"])
	return &lookupStage
}
