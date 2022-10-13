package svr

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/utmhikari/daggre/internal/cmd"
	"github.com/utmhikari/daggre/internal/svr/handler"
	"github.com/utmhikari/daggre/pkg/util"
	"log"
)

type ServerConfig struct {
	Port int `json:"port"`
}

func router() *gin.Engine {
	r := gin.Default()

	baseHandler := handler.Base
	r.GET("/ping", baseHandler.PingPong)
	r.GET("/health", baseHandler.HealthCheck)

	api := r.Group("/api")
	v1 := api.Group("/v1")

	aggreHandler := handler.Aggre
	v1.POST("/aggre", aggreHandler.Aggregate)

	return r
}

func Start() {
	log.Printf("svr params: %+v\n", cmd.SvrParams)

	cfgPath := cmd.SvrParams.CfgPath
	svrCfg := &ServerConfig{}
	err := util.ReadJsonFile(cfgPath, svrCfg)
	if err != nil {
		log.Panicf("failed to load server config from %s, %v\n", cfgPath, err)
	}

	r := router()
	addr := fmt.Sprintf(":%d", svrCfg.Port)
	err = r.Run(addr)
	if err != nil {
		log.Panicf("server error: %v", err)
	}
}
