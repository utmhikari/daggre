package daggre

import (
	"github.com/utmhikari/daggre/pkg/util"
	"log"
)

type FilterStage struct {
	Locator  string      `json:"locator"`
	Operator string      `json:"operator"`
	Value    interface{} `json:"value"`
}

func (f *FilterStage) Process(tb *Table, r *PipelineRule) *Table {
	log.Printf("filter stage: %+v\n", f)

	locator := NewLocatorFromString(f.Locator)
	if !locator.Valid() {
		return &Table{} // empty table
	}

	nextTb := Table{}
	for _, row := range *tb {
		locatedValue := locator.Locate(row)

		// comparison?
		comparison, ok := ComparisonCallbacks[f.Operator]
		if ok {
			if comparison(locatedValue, f.Value) {
				nextTb.AppendRow(row)
				continue
			}
		}
	}
	return &nextTb
}

func NewFilterStage(params PipelineStageParams) PipelineStageInterface {
	filterStage := FilterStage{}
	filterStage.Locator = util.ToString(params["locator"])
	filterStage.Operator = util.ToString(params["operator"])
	filterStage.Value = params["value"]
	return &filterStage
}
