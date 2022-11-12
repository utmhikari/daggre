package svr

import (
	"github.com/utmhikari/daggre/internal/svr/model"
	"github.com/utmhikari/daggre/pkg/daggre"
	"github.com/utmhikari/daggre/pkg/util"
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
	ret, err := client.RequestAggre(requestParams)
	if err != nil {
		t.Logf("request err -> %v\n", err)
		t.Fail()
		return
	}
	t.Logf("aggre stats: %s\n", util.JsonDump(ret.Stats))
	t.Logf("aggre output: %s\n", ret.Output.ToString())
	if !ret.Output.Equals(expectedOutput) {
		t.Logf("output differs from expected output: %s\n", expectedOutput.ToString())
		t.Fail()
		return
	}
	t.Logf("test filter successfully\n")
}

func TestLookup(t *testing.T) {
	data := &daggre.Data{
		"customer": &daggre.Table{
			{
				"id":      10000,
				"name":    "hikari",
				"hobbies": []string{"basketball", "football"},
				"from":    101,
			},
			{
				"id":      10001,
				"name":    "kobe",
				"hobbies": []string{"basketball", "rap"},
				"from":    202.0,
			},
			{
				"id":      10002,
				"name":    "ronaldo",
				"hobbies": []string{"football"},
				"from":    404,
			},
			{
				"id":      10003,
				"name":    "james",
				"hobbies": []string{"basketball"},
				"from":    303,
			},
		},
		"country": &daggre.Table{
			{
				"id":   101,
				"name": "china",
			},
			{
				"id":   202,
				"name": "usa",
			},
			{
				"id":   303.0,
				"name": "russia",
			},
		},
	}
	aggre := &daggre.Aggregator{
		Pipelines: []*daggre.Pipeline{
			{
				Name:   "country",
				Desc:   "join customers of country",
				Tables: []string{"country"},
				Stages: []*daggre.PipelineStage{
					{
						Name: "lookup",
						Params: map[string]interface{}{
							"fromPipeline":   "customer",
							"localLocator":   "id",
							"foreignLocator": "from",
							"toField":        "customers",
						},
					},
				},
			},
			{
				Name:   "customer",
				Desc:   "customer original table",
				Tables: []string{"customer"},
			},
		},
		Main: "country",
	}
	expectedOutput := &daggre.Table{
		{
			"customers": []map[string]interface{}{
				{
					"from": 101,
					"hobbies": []string{
						"basketball",
						"football",
					},
					"id":   10000,
					"name": "hikari",
				},
			},
			"id":   101,
			"name": "china",
		},
		{
			"customers": []map[string]interface{}{
				{
					"from": 202,
					"hobbies": []string{
						"basketball",
						"rap",
					},
					"id":   10001,
					"name": "kobe",
				},
			},
			"id":   202,
			"name": "usa",
		},
		{
			"customers": []map[string]interface{}{
				{
					"from": 303,
					"hobbies": []string{
						"basketball",
					},
					"id":   10003,
					"name": "james",
				},
			},
			"id":   303,
			"name": "russia",
		},
	}
	requestParams := &model.AggreParams{
		Data:  data,
		Aggre: aggre,
	}
	ret, err := client.RequestAggre(requestParams)
	if err != nil {
		t.Logf("request err -> %v\n", err)
		t.Fail()
		return
	}
	t.Logf("aggre stats: %s\n", util.JsonDump(ret.Stats))
	t.Logf("aggre output: %s\n", ret.Output.ToString())
	if !ret.Output.Equals(expectedOutput) {
		t.Logf("output differs from expected output: %s\n", expectedOutput.ToString())
		t.Fail()
		return
	}
	t.Logf("test lookup successfully\n")
}
