package client

import "fmt"

var Host = "http://localhost"
var Port = 8964

func MakeUrl(path string) string {
	return fmt.Sprintf("%s:%d%s", Host, Port, path)
}

type RespBody struct {
	Success bool        `json:"success"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
	Code    int         `json:"code"`
}
