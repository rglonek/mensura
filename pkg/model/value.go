// Package model holds the types shared by every Mensura component: the
// sample shape that travels on the wire, the value union that a field may
// hold, identifier validation, and the primary-key derivation.
package model

import (
	"fmt"
	"math"
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

// ValidateKind refuses a field kind this build does not act on.
//
// Nothing used to check it. A kind is a plain string all the way from a
// spec's `fields:` block, through the write API, into the catalogue and
// out to the query validator -- which compares it against the four
// constants above and simply does not recognise anything else. So
// `kind: couter` compiled, was accepted, was persisted, and then silently
// switched off every behaviour the declaration was written for: no W102
// "counter is plotted raw; consider RATE", no RATE pre-selection in the
// builder, and for `string` no E005 refusal of numeric modifiers and no
// string column under FORMAT table. A typo that changes nothing visible
// except the diagnostics is the one an operator never finds.
func ValidateKind(k Kind) error {
	switch k {
	case KindCounter, KindGauge, KindDelta, KindString:
		return nil
	}
	return fmt.Errorf("unknown kind %q: expected counter, gauge, delta or string", string(k))
}

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
//
// A non-finite result reports false, whichever type it came from.
// ValidateFieldValue refuses NaN and infinity on the write path, but
// strconv.ParseFloat accepts the literal text "NaN", "Inf" and
// "+Infinity" -- so a *string*-typed field carrying one of those tokens
// coerced to a non-finite float here, landed in wire.Series.Values, and
// made encoding/json fail on the response *after* the 200 header had
// been written: the panel received a truncated body with no status and no
// diagnostic to explain it. A value that cannot be plotted reads as an
// absent one instead, which is the same answer the render path already
// gives a column a row does not carry.
func (v Value) AsFloat() (float64, bool) {
	switch v.T {
	case TypeInt:
		return float64(v.I), true
	case TypeFloat:
		return v.F, isFinite(v.F)
	case TypeString:
		f, err := strconv.ParseFloat(v.S, 64)
		return f, err == nil && isFinite(f)
	case TypeBool:
		if v.B {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

// isFinite reports whether a float is a real number, as opposed to a NaN
// or an infinity.
func isFinite(f float64) bool { return !math.IsNaN(f) && !math.IsInf(f, 0) }

// IsFinite is isFinite for callers outside this package.
//
// The render walk is arithmetic on values this package has already
// screened, and arithmetic on finite inputs is not closed over the finite
// numbers: a DELTA across two values of opposing sign near the float64
// limit overflows to an infinity, and so does a PER SECOND division by a
// millisecond interval. Whatever produces one, it must not reach the wire,
// so the check belongs somewhere both the producer and the encoder can
// see it.
func IsFinite(f float64) bool { return isFinite(f) }

// maxIntFloat is 2^63: the first float64 magnitude that an int64 cannot
// hold. It is exactly representable, so the comparisons below are exact.
const maxIntFloat = float64(1 << 63)

// AsInt coerces to int64 where that is lossless-enough for indexing.
//
// A non-finite float reports false rather than being converted: the
// result of int64(NaN) is implementation-defined, and this is the
// function the engine reads a row's indexed timestamp through, so a
// garbage answer would place the row at a fabricated time that no range
// scan can find.
func (v Value) AsInt() (int64, bool) {
	switch v.T {
	case TypeInt:
		return v.I, true
	case TypeFloat:
		if !isFinite(v.F) {
			return 0, false
		}
		// Range-checked as well as finite-checked. Converting a float
		// outside the int64 range is undefined by the Go spec -- amd64
		// yields the indefinite value and arm64 saturates -- so a
		// bucket set whose `total_field` capture coerced to 1e300
		// produced a total nothing measured, and with it a `tail` and a
		// `<bucket>plus` column derived from it. A number this function
		// cannot represent is not a number it may guess at.
		if v.F >= maxIntFloat || v.F < -maxIntFloat {
			return 0, false
		}
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
