package handler

import (
	"github.com/gin-gonic/gin"
	"net/http"
)

const (
	ErrNil = 0
	ErrAny = -1
)

type Resp struct {
	success bool
	message string
	data    interface{}
	code    int

	c *gin.Context
}

func OKResp(c *gin.Context) *Resp {
	return &Resp{
		success: true,
		message: "",
		data:    nil,
		code:    ErrNil,
		c:       c,
	}
}

func ErrResp(c *gin.Context) *Resp {
	return &Resp{
		success: false,
		message: "",
		data:    nil,
		code:    ErrAny,
		c:       c,
	}
}

func (r *Resp) Message(msg string) *Resp {
	r.message = msg
	return r
}

func (r *Resp) Data(data interface{}) *Resp {
	r.data = data
	return r
}

func (r *Resp) ErrCode(code int) *Resp {
	if !r.success && code != ErrNil {
		r.code = code
	}
	return r
}

func (r *Resp) Response() {
	r.c.JSON(http.StatusOK, gin.H{
		"success": r.success,
		"message": r.message,
		"data":    r.data,
		"code":    r.code,
	})
}

type baseHandler struct{}

func (b *baseHandler) PingPong(c *gin.Context) {
	OKResp(c).Message("pong").Response()
}

func (b *baseHandler) HealthCheck(c *gin.Context) {
	OKResp(c).Message("ok").Response()
}

var Base = &baseHandler{}
