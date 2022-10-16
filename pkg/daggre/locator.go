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

var mapType = reflect.TypeOf(make(map[string]interface{}))

func isMapType(v interface{}) bool {
	tp := reflect.TypeOf(v)
	return tp == mapType
}

func (l *Locator) Locate(r *Row) interface{} {
	if r == nil || !l.Valid() {
		return nil
	}

	var ptr interface{} = r
	for _, field := range l.fields {
		if ptr == nil {
			break
		}
		if ptr != r {
			if !isMapType(ptr) {
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

func (l *Locator) Set(r *Row, v interface{}) bool {
	if r == nil || !l.Valid() {
		return false
	}

	var ptr interface{} = r

	for i, field := range l.fields {
		if ptr == nil {
			return false
		}
		if ptr != r {
			if !isMapType(ptr) {
				return false
			}
		}
		if i < len(l.fields)-1 {
			var nxt interface{}
			var ok bool
			if ptr == r {
				nxt, ok = (*r)[field]
			} else {
				nxt, ok = ptr.(map[string]interface{})[field]
			}
			if !ok {
				return false
			} else {
				ptr = nxt
			}
		} else {
			if ptr == r {
				(*r)[field] = v
			} else {
				ptr.(map[string]interface{})[field] = v
			}
		}
	}
	return true
}
