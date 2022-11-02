package daggre

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/utmhikari/daggre/pkg/util"
	"log"
)

type FilterStage struct {
	Locator  string      `json:"locator"`
	Operator string      `json:"operator"`
	Value    interface{} `json:"value"`
}

func (f *FilterStage) Process(tb *Table, a *Aggregator) *PipelineStageProcResult {
	log.Printf("filter stage: %s\n", util.JsonDump(f))
	ret := &PipelineStageProcResult{
		tb:  &Table{},
		err: nil,
	}

	locator := NewLocator(f.Locator)
	if !locator.Valid() {
		ret.err = errors.New(fmt.Sprintf("invalid locator expr: %s", f.Locator))
		return ret
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
	ret.tb = &nextTb
	return ret
}

func NewFilterStage(params PipelineStageParams) PipelineStageInterface {
	filterStage := FilterStage{}
	jsonString, _ := json.Marshal(params)
	_ = json.Unmarshal(jsonString, &filterStage)
	return &filterStage
}
