package daggre

import (
	"encoding/json"
	"fmt"
	"github.com/utmhikari/daggre/pkg/util"
	"log"
)

type UnwindStage struct {
	BasePipelineStage
	Locator                    string `json:"locator"`
	IncludeArrayIndex          string `json:"includeArrayIndex"`
	PreserveNullAndEmptyArrays bool   `json:"preserveNullAndEmptyArrays"`
}

func (u *UnwindStage) Check() error {
	locator := NewLocator(u.Locator)
	if !locator.Valid() {
		return fmt.Errorf("invalid locator %s", u.Locator)
	}
	return nil
}

type Array []interface{}

func (u *UnwindStage) Process(tb *Table, a *Aggregator) *PipelineStageProcResult {
	log.Printf("unwind stage: %s\n", util.JsonDump(u))

	ret := &PipelineStageProcResult{
		tb:  &Table{},
		err: nil,
	}
	nextTb := Table{}

	locator := NewLocator(u.Locator)
	for _, row := range *tb {
		parent, field, value := locator.LocateWithParent(row)

		if value == nil {
			if u.PreserveNullAndEmptyArrays {
				nextTb = append(nextTb, row)
				continue
			}
		}

		arrValueCopy := &Array{}
		copyErr := util.DeepCopyByJson(&value, arrValueCopy)
		if copyErr != nil {
			// log.Printf("copy err: " + copyErr.Error())
			if u.PreserveNullAndEmptyArrays {
				nextTb = append(nextTb, row)
				continue
			}
		}

		for i, valueCopy := range *arrValueCopy {
			parent[field] = valueCopy
			if len(u.IncludeArrayIndex) > 0 {
				parent[u.IncludeArrayIndex] = i
			}
			nextTb = append(nextTb, row.Copy())
		}
	}

	ret.tb = &nextTb
	return ret
}

func NewUnwindStage(params PipelineStageParams) PipelineStageInterface {
	unwindStage := &UnwindStage{}
	jsonString, _ := json.Marshal(params)
	_ = json.Unmarshal(jsonString, unwindStage)
	return unwindStage
}
