package daggre

import (
	"github.com/utmhikari/daggre/pkg/util"
	"log"
	"reflect"
)

type FilterStage struct {
	Locator  string      `json:"locator"`
	Operator string      `json:"operator"`
	Value    interface{} `json:"value"`
}

func (f *FilterStage) Process(tb *Table, r *PipelineRule) *Table {
	log.Printf("filter stage: %+v\n", f)

	// debugging usage
	if f.Operator != "==" {
		return tb
	}
	//if reflect.TypeOf(f.Value) != reflect.TypeOf("") {
	//	return tb
	//}

	nextTb := Table{}
	for _, row := range *tb {
		if reflect.DeepEqual(row[f.Locator], f.Value) {
			rowCopy := row.Copy()
			if rowCopy != nil {
				nextTb = append(nextTb, *rowCopy)
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
