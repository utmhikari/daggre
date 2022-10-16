package svr

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/utmhikari/daggre/internal/svr/handler"
	"github.com/utmhikari/daggre/pkg/util"
	"log"
)

type Cfg struct {
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

func Start(args *Args) {
	log.Printf("svr cmd args: %+v\n", args)

	cfgPath := args.CfgPath
	cfg := &Cfg{}
	err := util.ReadYamlFile(cfgPath, cfg)
	if err != nil {
		log.Panicf("failed to load server config from %s, %v\n", cfgPath, err)
	}
	log.Printf("svr cfg: %+v\n", cfg)

	r := router()
	addr := fmt.Sprintf(":%d", cfg.Port)
	err = r.Run(addr)
	if err != nil {
		log.Panicf("server error: %v", err)
	}
}
