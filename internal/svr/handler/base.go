package handler

import (
	"github.com/gin-gonic/gin"
)

type baseHandler struct{}

func (b *baseHandler) PingPong(c *gin.Context) {
	OKResp(c).SetMessage("pong").Response()
}

func (b *baseHandler) HealthCheck(c *gin.Context) {
	OKResp(c).SetMessage("ok").Response()
}

var Base = &baseHandler{}
