package svr

import (
	"github.com/utmhikari/daggre/internal/svr/model"
	"github.com/utmhikari/daggre/pkg/daggre"
	"github.com/utmhikari/daggre/res/testcases/svr/client"
	"testing"
)

func TestFilter(t *testing.T) {
	data := &daggre.Data{
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
	aggre := &daggre.Aggregator{
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
				},
			},
		},
		Main: "customer",
	}
	expectedOutput := &daggre.Table{
		{
			"name":    "hikari",
			"id":      -1,
			"hobbies": []interface{}{"basketball", "football"},
		},
		{
			"name":    "kobe",
			"id":      -2.5,
			"hobbies": []interface{}{"basketball", "rap"},
			"from":    "usa",
		},
	}
	requestParams := &model.AggreParams{
		Data:  data,
		Aggre: aggre,
	}
	output, err := client.RequestAggre(requestParams)
	if err != nil {
		t.Logf("request err -> %v\n", err)
		t.Fail()
		return
	}
	t.Logf("aggre output: %s\n", output.ToString())
	if !output.Equals(expectedOutput) {
		t.Logf("output differs from expected output: %s\n", expectedOutput.ToString())
		t.Fail()
		return
	}
	t.Logf("test filter successfully\n")
}
