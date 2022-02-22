package daggre

import (
	"reflect"
	"strings"
)

type Locator struct {
	fields []string
}

var locatorSep = "."

func NewLocator(s string) *Locator {
	splits := strings.Split(s, locatorSep)
	locator := &Locator{
		fields: []string{},
	}
	for _, sp := range splits {
		if len(sp) > 0 {
			locator.fields = append(locator.fields, sp)
		}
	}
	return locator
}

func (l *Locator) Valid() bool {
	return len(l.fields) > 0
}

func (l *Locator) Locate(r *Row) interface{} {
	if r == nil {
		return nil
	}

	if !l.Valid() {
		return nil
	}

	var ptr interface{} = r
	mapType := reflect.TypeOf(make(map[string]interface{}))
	for _, field := range l.fields {
		if ptr == nil {
			break
		}
		// TODO: implement IsMapType(obj interface{}) method
		if ptr != r {
			ptrType := reflect.TypeOf(ptr)
			if ptrType != reflect.TypeOf(mapType) && ptrType != reflect.TypeOf(r) {
				ptr = nil
				break
			}
		}

		var nxt interface{}
		var ok bool
		if ptr == r {
			nxt, ok = (*r)[field]
		} else {
			nxt, ok = ptr.(map[string]interface{})[field]
		}
		if !ok {
			ptr = nil
		} else {
			ptr = nxt
		}
	}
	return ptr
}
