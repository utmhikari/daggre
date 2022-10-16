package daggre

import (
	"encoding/json"
	"github.com/utmhikari/daggre/pkg/util"
	"log"
)

type FilterStage struct {
	Locator  string      `json:"locator"`
	Operator string      `json:"operator"`
	Value    interface{} `json:"value"`
}

func (f *FilterStage) Process(tb *Table, a *Aggregator) *Table {
	log.Printf("filter stage: %s\n", util.JsonDump(f))

	locator := NewLocator(f.Locator)
	if !locator.Valid() {
		return &Table{} // empty table
	}

	nextTb := Table{}
	for _, row := range *tb {
		locatedValue := locator.Locate(row)

		// compare?
		if Compare(locatedValue, f.Value, f.Operator) {
			nextTb.AppendRow(row)
			continue
		}

	}
	return &nextTb
}

func NewFilterStage(params PipelineStageParams) PipelineStageInterface {
	filterStage := FilterStage{}
	jsonString, _ := json.Marshal(params)
	_ = json.Unmarshal(jsonString, &filterStage)
	return &filterStage
}
