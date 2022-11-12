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

// LocateWithParent returns parent, field and value
func (l *Locator) LocateWithParent(r *Row) (map[string]interface{}, string, interface{}) {
	if r == nil || !l.Valid() {
		return nil, "", nil
	}

	var ptr map[string]interface{} = *r
	for i, field := range l.fields {
		if i == len(l.fields)-1 {
			value, _ := ptr[field]
			return ptr, field, value
		}

		nxt, ok := ptr[field]
		if !ok || !isMapType(nxt) {
			return nil, "", nil
		}
		ptr = nxt.(map[string]interface{})
	}

	// UNREACHABLE
	return nil, "", nil
}

func (l *Locator) Locate(r *Row) interface{} {
	_, _, value := l.LocateWithParent(r)
	return value
}

func (l *Locator) Set(r *Row, v interface{}) bool {
	parent, field, _ := l.LocateWithParent(r)
	if parent == nil {
		return false
	}
	parent[field] = v
	return true
}
