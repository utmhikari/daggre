package worker

import (
	"github.com/utmhikari/daggre/pkg/daggre"
	daggreService "github.com/utmhikari/daggre/pkg/daggre/service"
	"github.com/utmhikari/daggre/pkg/util"
	"sync"
	"testing"
	"time"
)

var filterData = &daggre.Data{
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

var filterAggre = &daggre.Aggregator{
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

var filterExpectedOutput = &daggre.Table{
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

func TestWorkerFilter(t *testing.T) {
	worker, err := daggreService.NewWorker(1, 5*time.Second)
	if err != nil {
		t.Fatalf("init worker failed: %v", err)
		return
	}
	worker.Start()
	defer worker.Stop()
	ret, err := worker.Aggregate(filterData, filterAggre)
	if err != nil {
		t.Logf("aggre err -> %v\n", err)
		t.Fail()
		return
	}
	t.Logf("aggre stats: %s\n", util.JsonDump(ret.Stats))
	t.Logf("aggre output: %s\n", ret.Output.ToString())
	if !ret.Output.Equals(filterExpectedOutput) {
		t.Logf("output differs from expected output: %s\n", filterExpectedOutput.ToString())
		t.Fail()
		return
	}
	t.Logf("test filter successfully\n")
}

func TestWorkerFilterMultiple(t *testing.T) {
	worker, err := daggreService.NewWorker(3, 5*time.Second)
	if err != nil {
		t.Fatalf("init worker failed: %v", err)
		return
	}
	worker.Start()
	defer worker.Stop()

	var wg sync.WaitGroup
	numTasks := 10
	wg.Add(numTasks)
	for i := 0; i < numTasks; i++ {
		go func(taskNum int) {
			defer wg.Done()
			t.Logf("run test %d,", taskNum)
			ret, err := worker.Aggregate(filterData, filterAggre)
			if err != nil {
				t.Logf("aggre fail -> %v\n", err)
				t.Fail()
				return
			}
			t.Logf("aggre stats: %s\n", util.JsonDump(ret.Stats))
			t.Logf("aggre output: %s\n", ret.Output.ToString())
			if !ret.Output.Equals(filterExpectedOutput) {
				t.Logf("output differs from expected output: %s\n", filterExpectedOutput.ToString())
				t.Fail()
				return
			}
			t.Logf("test filter successfully\n")
		}(i + 1)
	}
	wg.Wait()
}

var lookupData = &daggre.Data{
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

var lookupAggre = &daggre.Aggregator{
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

var lookupExpectedOutput = &daggre.Table{
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

func TestWorkerLookup(t *testing.T) {
	worker, err := daggreService.NewWorker(1, 5*time.Second)
	if err != nil {
		t.Fatalf("init worker failed: %v", err)
		return
	}
	worker.Start()
	defer worker.Stop()
	ret, err := worker.Aggregate(lookupData, lookupAggre)
	if err != nil {
		t.Logf("aggre err -> %v\n", err)
		t.Fail()
		return
	}
	t.Logf("aggre stats: %s\n", util.JsonDump(ret.Stats))
	t.Logf("aggre output: %s\n", ret.Output.ToString())
	if !ret.Output.Equals(lookupExpectedOutput) {
		t.Logf("output differs from expected output: %s\n", lookupExpectedOutput.ToString())
		t.Fail()
		return
	}
	t.Logf("test lookup successfully\n")
}
