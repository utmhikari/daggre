package daggre

import (
	"encoding/json"
	"fmt"
	"github.com/utmhikari/daggre/pkg/util"
	"log"
)

type FilterStage struct {
	BasePipelineStage
	Locator  string      `json:"locator"`
	Operator string      `json:"operator"`
	Value    interface{} `json:"value"`
}

func (f *FilterStage) Check() error {
	locator := NewLocator(f.Locator)
	if !locator.Valid() {
		return fmt.Errorf("invalid locator %s", f.Locator)
	}
	if !IsComparator(f.Operator) {
		return fmt.Errorf("invalid comparator %s", f.Operator)
	}
	return nil
}

func (f *FilterStage) Process(tb *Table, a *Aggregator) *PipelineStageProcResult {
	log.Printf("filter stage: %s\n", util.JsonDump(f))
	ret := &PipelineStageProcResult{
		tb:  &Table{},
		err: nil,
	}
	locator := NewLocator(f.Locator)

	for _, row := range *tb {
		locatedValue := locator.Locate(row)

		// compare?
		if Compare(locatedValue, f.Value, f.Operator) {
			ret.tb.AppendRow(row)
			continue
		}
	}
	return ret
}

func NewFilterStage(params PipelineStageParams) PipelineStageInterface {
	filterStage := FilterStage{}
	jsonString, _ := json.Marshal(params)
	_ = json.Unmarshal(jsonString, &filterStage)
	return &filterStage
}
