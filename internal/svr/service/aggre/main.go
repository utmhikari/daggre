package aggre

import (
	"github.com/utmhikari/daggre/internal/svr/model"
	"github.com/utmhikari/daggre/pkg/daggre"
)

func DoAggregate(params *model.AggreParams) *daggre.AggregateResult {
	data := params.Data
	aggre := params.Aggre
	return aggre.Aggregate(data)
}
