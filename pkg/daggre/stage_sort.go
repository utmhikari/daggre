package daggre

import (
	"encoding/json"
	"fmt"
	"github.com/utmhikari/daggre/pkg/util"
	"log"
	"sort"
)

const (
	AscOrder  = 1
	DescOrder = -1
)

type SortRule struct {
	Locator string `json:"locator"`
	Order   int    `json:"order"`
}

type SortStage struct {
	BasePipelineStage
	Rules []*SortRule `json:"rules"`
}

func (s *SortStage) Check() error {
	numRules := len(s.Rules)
	for i := 0; i < numRules; i++ {
		rule := s.Rules[i]
		ruleLocator := NewLocator(rule.Locator)
		if !ruleLocator.Valid() {
			return fmt.Errorf("rule %d contains invalid locator %s", i+1, rule.Locator)
		}
		order := rule.Order
		if order != AscOrder && order != DescOrder {
			return fmt.Errorf(
				"rule %d contains invalid order %d, expected AscOrder %d or DescOrder %d",
				i+1,
				order,
				AscOrder,
				DescOrder)
		}
	}
	return nil
}

func (s *SortStage) Process(tb *Table, a *Aggregator) *PipelineStageProcResult {
	log.Printf("sort stage: %s\n", util.JsonDump(s))

	ret := &PipelineStageProcResult{
		tb:  &Table{},
		err: nil,
	}

	// initialize all locators
	var locators []*Locator
	for _, rule := range s.Rules {
		locator := NewLocator(rule.Locator)
		locators = append(locators, locator)
	}

	// do sort
	sort.Slice(*tb, func(i, j int) bool {
		row1, row2 := (*tb)[i], (*tb)[j]
		for idx, rule := range s.Rules {
			order := rule.Order
			if order != AscOrder && order != DescOrder {
				continue // ignore rule if order value is invalid
			}
			locator := locators[idx]
			if !locator.Valid() {
				continue // ignore rule if locator is invalid
			}
			v1, v2 := locator.Locate(row1), locator.Locate(row2)
			if !CanCompareOrder(v1, v2) {
				continue // ignore values if values cannot be ordered
			}
			if order == AscOrder {
				isLt := Compare(v1, v2, ComparisonOperatorLt)
				if isLt {
					return true
				}
			} else {
				isGt := Compare(v1, v2, ComparisonOperatorGt)
				if isGt {
					return true
				}
			}
			isEq := Compare(v1, v2, ComparisonOperatorEq)
			if isEq {
				continue
			} else {
				return false
			}
		}
		return false
	})

	ret.tb = tb
	return ret
}

func NewSortStage(params PipelineStageParams) PipelineStageInterface {
	sortStage := &SortStage{}
	jsonString, _ := json.Marshal(params)
	_ = json.Unmarshal(jsonString, sortStage)
	return sortStage
}
