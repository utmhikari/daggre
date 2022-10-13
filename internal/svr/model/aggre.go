package model

import "github.com/utmhikari/daggre/pkg/daggre"

type AggreParams struct {
	Data  daggre.Data       `json:"data" binding:"required"`
	Aggre daggre.Aggregator `json:"aggre" binding:"required"`
}
