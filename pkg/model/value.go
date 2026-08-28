// Package model holds the types shared by every Mensura component: the
// sample shape that travels on the wire, the value union that a field may
// hold, identifier validation, and the primary-key derivation.
package model

import (
	"fmt"
	"strconv"
)

// Kind classifies a field so the query builder can pick sane defaults and
// the validator can reject nonsensical modifiers.
type Kind string

const (
	KindCounter Kind = "counter" // monotonic; rendering usually wants RATE
	KindGauge   Kind = "gauge"   // instantaneous measurement
	KindDelta   Kind = "delta"   // already a per-interval difference
	KindString  Kind = "string"  // table/logs payload, never plotted
)

// ValueType is the storage type of a field value.
type ValueType uint8

const (
	TypeInvalid ValueType = iota
	TypeInt
	TypeFloat
	TypeString
	TypeBool
)

// Value is a typed field value. It is a small struct rather than an
// interface so that a row of them costs no allocations.
type Value struct {
	T ValueType
	I int64
	F float64
	S string
	B bool
}

func Int(v int64) Value     { return Value{T: TypeInt, I: v} }
func Float(v float64) Value { return Value{T: TypeFloat, F: v} }
func String(v string) Value { return Value{T: TypeString, S: v} }
func Bool(v bool) Value     { return Value{T: TypeBool, B: v} }

func (v Value) Valid() bool { return v.T != TypeInvalid }

// AsFloat coerces to float64 for the render path. Numeric strings are
// accepted because extraction may legitimately leave a value as a string
// when it could not be coerced earlier; anything else reports false.
func (v Value) AsFloat() (float64, bool) {
	switch v.T {
	case TypeInt:
		return float64(v.I), true
	case TypeFloat:
		return v.F, true
	case TypeString:
		f, err := strconv.ParseFloat(v.S, 64)
		return f, err == nil
	case TypeBool:
		if v.B {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

// AsInt coerces to int64 where that is lossless-enough for indexing.
func (v Value) AsInt() (int64, bool) {
	switch v.T {
	case TypeInt:
		return v.I, true
	case TypeFloat:
		return int64(v.F), true
	case TypeString:
		i, err := strconv.ParseInt(v.S, 10, 64)
		return i, err == nil
	}
	return 0, false
}

func (v Value) String() string {
	switch v.T {
	case TypeInt:
		return strconv.FormatInt(v.I, 10)
	case TypeFloat:
		return strconv.FormatFloat(v.F, 'g', -1, 64)
	case TypeString:
		return v.S
	case TypeBool:
		return strconv.FormatBool(v.B)
	}
	return ""
}

// Coerce turns a scanned string into the narrowest type that parses,
// which is what keeps extracted counters as integers rather than floats.
func Coerce(s string) Value {
	if i, err := strconv.ParseInt(s, 10, 64); err == nil {
		return Int(i)
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return Float(f)
	}
	return String(s)
}

// ParseValueType maps a spec/catalogue type name onto a ValueType.
func ParseValueType(s string) (ValueType, error) {
	switch s {
	case "int", "int64":
		return TypeInt, nil
	case "float", "float64":
		return TypeFloat, nil
	case "string":
		return TypeString, nil
	case "bool":
		return TypeBool, nil
	}
	return TypeInvalid, fmt.Errorf("unknown value type %q", s)
}

func (t ValueType) String() string {
	switch t {
	case TypeInt:
		return "int64"
	case TypeFloat:
		return "float64"
	case TypeString:
		return "string"
	case TypeBool:
		return "bool"
	}
	return "invalid"
}
