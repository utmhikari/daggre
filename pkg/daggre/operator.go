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

func ComparisonEq(a, b interface{}) bool {
	return reflect.DeepEqual(a, b)
}

func ComparisonNe(a, b interface{}) bool {
	return !reflect.DeepEqual(a, b)
}

func ComparisonCommon(a, b interface{}, operator string) bool {
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

var ComparisonCallbacks = map[string]func(a, b interface{}) bool{
	ComparisonOperatorEq: ComparisonEq,
	ComparisonOperatorNe: ComparisonNe,
	ComparisonOperatorGt: func(a, b interface{}) bool { return ComparisonCommon(a, b, ComparisonOperatorGt) },
	ComparisonOperatorGe: func(a, b interface{}) bool { return ComparisonCommon(a, b, ComparisonOperatorGe) },
	ComparisonOperatorLt: func(a, b interface{}) bool { return ComparisonCommon(a, b, ComparisonOperatorLt) },
	ComparisonOperatorLe: func(a, b interface{}) bool { return ComparisonCommon(a, b, ComparisonOperatorLe) },
}
