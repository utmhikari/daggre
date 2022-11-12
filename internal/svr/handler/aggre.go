package handler

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/utmhikari/daggre/internal/svr/model"
	aggreService "github.com/utmhikari/daggre/internal/svr/service/aggre"
	"github.com/utmhikari/daggre/pkg/util"
	"log"
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
	aggreResult := aggreService.Aggregate(&aggreParams)
	log.Printf("aggre result: %s\n", util.JsonDump(aggreResult))
	OKResp(c).SetMessage("aggregate finished!").SetData(aggreResult).Response()
}

var Aggre = &aggreHandler{}
