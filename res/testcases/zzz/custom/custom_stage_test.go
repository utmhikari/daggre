package custom

import (
	"encoding/json"
	"errors"
	"github.com/utmhikari/daggre/pkg/daggre"
	"github.com/utmhikari/daggre/pkg/util"
	"log"
	"reflect"
	"strings"
	"testing"
)

var customStageData = &daggre.Data{
	"customer": &daggre.Table{
		{
			"id":      -1.0,
			"name":    "hikari",
			"hobbies": []string{"basketball", "football"},
		},
		{
			"id":      -2.5,
			"name":    "kobe",
			"hobbies": []string{"basketball", "rap"},
			"from":    "usa",
		},
		{
			"id":      0.5,
			"name":    "ronaldo",
			"hobbies": []string{"football"},
			"from":    "portugal",
		},
	},
}

var customStageAggre = &daggre.Aggregator{
	Pipelines: []*daggre.Pipeline{
		{
			Name:   "customer",
			Desc:   "filtered customer table",
			Tables: []string{"customer"},
			Stages: []*daggre.PipelineStage{
				{
					Name: "filter",
					Params: map[string]interface{}{
						"locator":  "id",
						"operator": "<=",
						"value":    -1,
					},
				},
				{
					Name: "keywordFilter",
					Params: map[string]interface{}{
						"field":   "name",
						"keyword": "hika",
						"partial": true,
					},
				},
			},
		},
	},
	Main: "customer",
}

var customStageExpectedOutput = &daggre.Table{
	{
		"name":    "hikari",
		"id":      -1,
		"hobbies": []interface{}{"basketball", "football"},
	},
}

type KeywordFilterStage struct {
	daggre.BasePipelineStage
	Field   string `json:"field"`
	Keyword string `json:"keyword"`
	Partial bool   `json:"partial"`
}

func (kf *KeywordFilterStage) Check() error {
	if len(kf.Field) == 0 {
		return errors.New("empty field")
	}
	if len(kf.Keyword) == 0 {
		return errors.New("empty keyword")
	}
	return nil
}

func (kf *KeywordFilterStage) Process(tb *daggre.Table, a *daggre.Aggregator) *daggre.PipelineStageProcResult {
	log.Printf("keyword filter stage: %s\n", util.JsonDump(kf))
	ret := daggre.NewPipelineStageProcResult()

	strType := reflect.TypeOf("")
	for _, row := range *tb {
		value, ok := (*row)[kf.Field]
		if !ok {
			continue
		}

		log.Printf("%s -> %v", kf.Field, value)
		if reflect.TypeOf(value) != strType {
			log.Printf("value is not of string type!")
			continue
		}

		s := value.(string)
		if (!kf.Partial && s == kf.Keyword) || (kf.Partial && strings.Contains(s, kf.Keyword)) {
			log.Printf("append row -> %+v", row)
			ret.GetTable().AppendRow(row)
		}
	}
	return ret
}

func NewKeywordFilterStage(params daggre.PipelineStageParams) daggre.PipelineStageInterface {
	keywordFilterStage := KeywordFilterStage{}
	jsonString, _ := json.Marshal(params)
	_ = json.Unmarshal(jsonString, &keywordFilterStage)
	return &keywordFilterStage
}

func TestCustomStageFilter(t *testing.T) {
	// register keyword filter stage
	daggre.RegisterPipelineStage("keywordFilter", NewKeywordFilterStage)

	ret := customStageAggre.Aggregate(customStageData)
	t.Logf("aggre stats: %s\n", util.JsonDump(ret.Stats))
	t.Logf("aggre output: %s\n", ret.Output.ToString())
	if !ret.Output.Equals(customStageExpectedOutput) {
		t.Logf("output differs from expected output: %s\n", customStageExpectedOutput.ToString())
		t.Fail()
		return
	}
	t.Logf("test filter successfully\n")
}
