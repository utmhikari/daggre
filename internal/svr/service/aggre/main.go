package aggre

import (
	"github.com/utmhikari/daggre/internal/svr/model"
	"github.com/utmhikari/daggre/pkg/daggre"
)

func Aggregate(params *model.AggreParams) *daggre.AggreResult {
	data := params.Data
	aggre := params.Aggre
	return aggre.Aggregate(data)
}
