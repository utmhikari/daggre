package util

import (
	"encoding/json"
)

func DeepCopyByJson(src interface{}, dest interface{}) error {
	byt, err := json.Marshal(src)
	if err != nil {
		return err
	}
	err = json.Unmarshal(byt, dest)
	if err != nil {
		return err
	}
	return nil
}

var jsonDumpPrefix = ""
var jsonDumpIndent = "  "

func JsonDumpSafe(v interface{}) (string, error) {
	bytes, err := json.MarshalIndent(v, jsonDumpPrefix, jsonDumpIndent)
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}

func JsonDump(v interface{}) string {
	s, _ := JsonDumpSafe(v)
	return s
}
