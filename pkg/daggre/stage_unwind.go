package daggre

import (
	"encoding/json"
	"github.com/utmhikari/daggre/pkg/util"
	"log"
)

type UnwindStage struct {
	Locator                    string `json:"locator"`
	IncludeArrayIndex          string `json:"includeArrayIndex"`
	PreserveNullAndEmptyArrays bool   `json:"preserveNullAndEmptyArrays"`
}

type Array []interface{}

func (u *UnwindStage) Process(tb *Table, a *Aggregator) *Table {
	log.Printf("unwind stage: %s\n", util.JsonDump(u))

	nextTb := Table{}

	locator := NewLocator(u.Locator)
	if !locator.Valid() {
		return &nextTb
	}

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

	return &nextTb
}

func NewUnwindStage(params PipelineStageParams) PipelineStageInterface {
	unwindStage := &UnwindStage{}
	jsonString, _ := json.Marshal(params)
	_ = json.Unmarshal(jsonString, unwindStage)
	return unwindStage
}
