package client

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/utmhikari/daggre/internal/svr/model"
	"github.com/utmhikari/daggre/pkg/daggre"
	"io/ioutil"
	"log"
	"net/http"
)

var aggreUrl = MakeUrl("/api/v1/aggre")

func RequestAggre(params *model.AggreParams) (*daggre.AggregateResult, error) {
	reqBody, err := json.Marshal(params)
	if err != nil {
		return nil, errors.New("reqbody err: " + err.Error())
	}

	resp, err := http.Post(aggreUrl, "application/json", bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, errors.New("post aggre err: " + err.Error())
	}
	defer func() {
		err = resp.Body.Close()
		if err != nil {
			log.Printf("aggre resp body close error: %v\n", err)
		}
	}()

	respBodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.New("read resp body bytes err: " + err.Error())
	}

	respBody := &RespBody{}
	err = json.Unmarshal(respBodyBytes, &respBody)
	if err != nil {
		return nil, errors.New("unmarshal resp body err: " + err.Error())
	}
	log.Printf("resp body: %+v\n", respBody)
	if !respBody.Success {
		return nil, errors.New("resp fail: " + respBody.Message)
	}

	respDataBytes, err := json.Marshal(respBody.Data)
	if err != nil {
		return nil, errors.New("cannot marshal resp data to json bytes: " + err.Error())
	}
	ret := &daggre.AggregateResult{}
	err = json.Unmarshal(respDataBytes, ret)
	if err != nil {
		return nil, errors.New("cannot convert resp data to aggre result: " + err.Error())
	}
	return ret, nil
}
