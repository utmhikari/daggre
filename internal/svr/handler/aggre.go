package handler

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/utmhikari/daggre/internal/svr/model"
	aggreService "github.com/utmhikari/daggre/internal/svr/service/aggre"
	"log"
	"time"
)

type aggreHandler struct{}

func (a *aggreHandler) Aggregate(c *gin.Context) {
	aggreParams := model.AggreParams{}
	if err := c.ShouldBindJSON(&aggreParams); err != nil {
		log.Printf("AggreParams error: %v\n", err)
		ErrResp(c).
			SetMessage(fmt.Sprintf("params error: %v\n", err)).
			Response()
		return
	}

	startTime := time.Now()

	data, err := aggreService.DoAggregate(&aggreParams)
	if err != nil {
		log.Printf("DoAggregate error: %v\n", err)
		ErrResp(c).
			SetMessage(fmt.Sprintf("aggregation error: %v\n", err)).
			Response()
		return
	}

	endTime := time.Now()
	deltaTime := endTime.Sub(startTime)
	log.Printf("aggregation elapsed %d milliseconds\n", deltaTime.Milliseconds())
	OKResp(c).SetMessage("aggregate successfully!").SetData(data).Response()
}

var Aggre = &aggreHandler{}
