package daggre

import "reflect"

const (
	ComparisonOperatorEq = "=="
	ComparisonOperatorNe = "!="
	ComparisonOperatorGt = ">"
	ComparisonOperatorGe = ">="
	ComparisonOperatorLt = "<"
	ComparisonOperatorLe = "<="
)

var (
	StringType = reflect.TypeOf("")
	IntType    = reflect.TypeOf(0)
	FloatType  = reflect.TypeOf(0.0)
)

func compareEq(a, b interface{}) bool {
	return reflect.DeepEqual(a, b)
}

func compareNe(a, b interface{}) bool {
	return !reflect.DeepEqual(a, b)
}

func compareCommon(a, b interface{}, operator string) bool {
	if a == nil || b == nil {
		return false
	}
	tp := reflect.TypeOf(a)
	if tp != reflect.TypeOf(b) {
		return false
	}

	// TODO: auto-gencode?
	switch operator {
	case ComparisonOperatorGt:
		switch tp {
		case StringType:
			return a.(string) > b.(string)
		case IntType:
			return a.(int) > b.(int)
		case FloatType:
			return a.(float64) > b.(float64)
		default:
			break
		}

	case ComparisonOperatorGe:
		switch tp {
		case StringType:
			return a.(string) >= b.(string)
		case IntType:
			return a.(int) >= b.(int)
		case FloatType:
			return a.(float64) >= b.(float64)
		default:
			break
		}
	case ComparisonOperatorLe:
		switch tp {
		case StringType:
			return a.(string) <= b.(string)
		case IntType:
			return a.(int) <= b.(int)
		case FloatType:
			return a.(float64) <= b.(float64)
		default:
			break
		}
	case ComparisonOperatorLt:
		switch tp {
		case StringType:
			return a.(string) < b.(string)
		case IntType:
			return a.(int) < b.(int)
		case FloatType:
			return a.(float64) < b.(float64)
		default:
			break
		}
	default:
		break
	}
	return false
}

var comparators = map[string]func(a, b interface{}) bool{
	ComparisonOperatorEq: compareEq,
	ComparisonOperatorNe: compareNe,
	ComparisonOperatorGt: func(a, b interface{}) bool { return compareCommon(a, b, ComparisonOperatorGt) },
	ComparisonOperatorGe: func(a, b interface{}) bool { return compareCommon(a, b, ComparisonOperatorGe) },
	ComparisonOperatorLt: func(a, b interface{}) bool { return compareCommon(a, b, ComparisonOperatorLt) },
	ComparisonOperatorLe: func(a, b interface{}) bool { return compareCommon(a, b, ComparisonOperatorLe) },
}

func Compare(a, b interface{}, operator string) bool {
	// comparison?
	comparator, ok := comparators[operator]
	if !ok {
		return false
	}
	return comparator(a, b)
}
