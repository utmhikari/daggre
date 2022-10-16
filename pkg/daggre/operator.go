package daggre

import (
	"github.com/utmhikari/daggre/pkg/util"
	"golang.org/x/exp/constraints"
	"reflect"
	"strconv"
)

const (
	ComparisonOperatorEq = "=="
	ComparisonOperatorNe = "!="
	ComparisonOperatorGt = ">"
	ComparisonOperatorGe = ">="
	ComparisonOperatorLt = "<"
	ComparisonOperatorLe = "<="
)

func compareEq(a, b interface{}) bool {
	return reflect.DeepEqual(a, b)
}

func compareNe(a, b interface{}) bool {
	return !reflect.DeepEqual(a, b)
}

func compareOrderImpl[T constraints.Ordered](a, b T, operator string) bool {
	switch operator {
	case ComparisonOperatorGt:
		return a > b
	case ComparisonOperatorGe:
		return a >= b
	case ComparisonOperatorLt:
		return a < b
	case ComparisonOperatorLe:
		return a <= b
	default:
		return false
	}
}

func compareOrder(a, b interface{}, operator string) bool {
	// check nil
	if a == nil || b == nil {
		return false
	}

	// dispatch by type & kind
	aType, bType := reflect.TypeOf(a), reflect.TypeOf(b)
	aKind, bKind := aType.Kind(), bType.Kind()

	// string
	if aKind == reflect.String && bKind == reflect.String {
		return compareOrderImpl[string](a.(string), b.(string), operator)
	}

	// number: convert to float64 in all
	if aKind >= reflect.Int && aKind <= reflect.Float64 && bKind >= reflect.Int && bKind <= reflect.Float64 {
		aString, bString := util.ToString(a), util.ToString(b)
		aValue, aErr := strconv.ParseFloat(aString, 64)
		if aErr != nil {
			return false
		}
		bValue, bErr := strconv.ParseFloat(bString, 64)
		if bErr != nil {
			return false
		}
		return compareOrderImpl[float64](aValue, bValue, operator)
	}

	// unsupported
	return false

}

var comparators = map[string]func(a, b interface{}) bool{
	ComparisonOperatorEq: compareEq,
	ComparisonOperatorNe: compareNe,
	ComparisonOperatorGt: func(a, b interface{}) bool { return compareOrder(a, b, ComparisonOperatorGt) },
	ComparisonOperatorGe: func(a, b interface{}) bool { return compareOrder(a, b, ComparisonOperatorGe) },
	ComparisonOperatorLt: func(a, b interface{}) bool { return compareOrder(a, b, ComparisonOperatorLt) },
	ComparisonOperatorLe: func(a, b interface{}) bool { return compareOrder(a, b, ComparisonOperatorLe) },
}

func Compare(a, b interface{}, operator string) bool {
	// comparison?
	comparator, ok := comparators[operator]
	if !ok {
		return false
	}
	return comparator(a, b)
}

func CanCompareOrder(a, b interface{}) bool {
	aType, bType := reflect.TypeOf(a), reflect.TypeOf(b)
	aKind, bKind := aType.Kind(), bType.Kind()
	if aKind == reflect.String && bKind == reflect.String {
		return true
	}
	if aKind >= reflect.Int && aKind <= reflect.Float64 && bKind >= reflect.Int && bKind <= reflect.Float64 {
		return true
	}
	return false
}
