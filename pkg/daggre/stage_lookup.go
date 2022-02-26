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

func (j *LookupStage) Process(tb *Table, a *Aggregator) *Table {
	log.Printf("join stage: %+v\n", j)

	// TODO: validation
	if len(j.ToField) == 0 {
		return &Table{}
	}

	fromTb, err := a.GetPipelineData(j.FromPipeline)
	if err != nil {
		log.Printf("err at pipeline %s -> %s\n", j.FromPipeline, err.Error())
		return &Table{}
	}

	foreignLocator := NewLocator(j.ForeignLocator)
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

	localLocator := NewLocator(j.LocalLocator)
	for _, row := range *tb {
		locatedValue := localLocator.Locate(row)
		rows, ok := fromTbRowMap[locatedValue]
		if ok {
			var newRows []*Row
			for _, r := range rows {
				newRows = append(newRows, r.Copy())
			}
			(*row)[j.ToField] = newRows
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
