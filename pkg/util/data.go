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
