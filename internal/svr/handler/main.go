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

func (r *Resp) SetMessage(msg string) *Resp {
	r.message = msg
	return r
}

func (r *Resp) SetData(data interface{}) *Resp {
	r.data = data
	return r
}

func (r *Resp) SetCode(code int) *Resp {
	if !r.success && code != ErrNil {
		r.code = code
	}
	return r
}

func (r *Resp) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"success": r.success,
		"message": r.message,
		"data":    r.data,
		"code":    r.code,
	}
}

func (r *Resp) Response() {
	r.c.JSON(http.StatusOK, r.ToMap())
}
