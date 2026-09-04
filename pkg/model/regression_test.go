package model

import (
	"encoding/json"
	"math"
	"testing"
)

// Two distinct label sets must never derive the same row key. They used
// to: the key hashed "k=v\x00" per label with no length prefix, and a
// label value may contain both the '=' and the NUL that framed it, so
// {a:"b", c:"d"} and {a:"b\x00c=d"} collided. PutBatch assumes a key is
// new or carries the same indexed value, so the second sample silently
// overwrote the first while the write API counted both as accepted.
func TestPrimaryKeySeparatesAmbiguousLabelSets(t *testing.T) {
	cases := [][2]*Sample{
		{
			{TSMs: 5, Labels: map[string]string{"a": "b", "c": "d"}, Fields: map[string]Value{"x": Int(1)}},
			{TSMs: 5, Labels: map[string]string{"a": "b\x00c=d"}, Fields: map[string]Value{"x": Int(1)}},
		},
		{
			{TSMs: 5, Labels: map[string]string{"ab": "c"}, Fields: map[string]Value{"x": Int(1)}},
			{TSMs: 5, Labels: map[string]string{"a": "bc"}, Fields: map[string]Value{"x": Int(1)}},
		},
	}
	for i, c := range cases {
		if PrimaryKey("s", c[0], KeyContent) == PrimaryKey("s", c[1], KeyContent) {
			t.Errorf("case %d: distinct samples share a content key", i)
		}
	}
}

// The same ambiguity existed on the field side of the content key, where
// a string value may carry the framing bytes too.
func TestPrimaryKeySeparatesAmbiguousFieldSets(t *testing.T) {
	a := &Sample{TSMs: 5, Fields: map[string]Value{"a": String("b"), "c": String("d")}}
	b := &Sample{TSMs: 5, Fields: map[string]Value{"a": String("b\x00c=" + string(rune(TypeString)) + "d")}}
	if PrimaryKey("s", a, KeyContent) == PrimaryKey("s", b, KeyContent) {
		t.Fatal("distinct field sets share a content key")
	}
}

// The set name is length-prefixed for the same reason: it must not be
// able to run into the timestamp that follows it.
func TestPrimaryKeySeparatesSetFromTimestamp(t *testing.T) {
	s := &Sample{TSMs: 1, Fields: map[string]Value{"x": Int(1)}}
	if PrimaryKey("ab", s, KeyContent) == PrimaryKey("a", s, KeyContent) {
		t.Fatal("distinct set names share a content key")
	}
}

// Offset keying must still separate two hints, and must not collide with
// a content key for the same sample.
func TestPrimaryKeyOffsetHintIsHonoured(t *testing.T) {
	a := &Sample{TSMs: 5, Fields: map[string]Value{"x": Int(1)}, KeyHint: "s\x0010"}
	b := &Sample{TSMs: 5, Fields: map[string]Value{"x": Int(1)}, KeyHint: "s\x0011"}
	if PrimaryKey("s", a, KeyOffset) == PrimaryKey("s", b, KeyOffset) {
		t.Fatal("distinct offset hints share a row key")
	}
}

// strconv.ParseFloat accepts "NaN" and "Inf", so Coerce turns those
// tokens in a log line into a float64 that encoding/json refuses. The
// failure lands on the request rather than the sample, so one such line
// used to make a whole batch permanently undeliverable -- and an
// undeliverable batch reads as a hole, which freezes every followed
// file's checkpoint for the life of the process. It has to be a named
// rejection instead.
func TestNonFiniteFieldValueIsRejectedByName(t *testing.T) {
	for _, text := range []string{"NaN", "Inf", "+Inf", "-Infinity"} {
		v := Coerce(text)
		if v.T != TypeFloat {
			t.Fatalf("Coerce(%q) produced %v, expected a float", text, v.T)
		}
		s := &Sample{TSMs: 1, Fields: map[string]Value{"latency": v}}
		err := s.Validate()
		if err == nil {
			t.Fatalf("Coerce(%q) was accepted by Validate", text)
		}
		if !contains(err.Error(), "latency") {
			t.Errorf("rejection for %q does not name the field: %v", text, err)
		}
	}
}

// Marshalling must fail loudly rather than emitting something a decoder
// would read back as a number.
func TestNonFiniteValueDoesNotMarshalSilently(t *testing.T) {
	for _, f := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if _, err := json.Marshal(Float(f)); err == nil {
			t.Errorf("%v marshalled without error", f)
		}
	}
	// Finite values are untouched.
	b, err := json.Marshal(Float(1.5))
	if err != nil || string(b) != `{"f":1.5}` {
		t.Fatalf("Float(1.5) marshalled as %s (%v)", b, err)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// The write API decodes with DisallowUnknownFields so a typo fails loudly,
// but Value has a custom unmarshaller, and a custom unmarshaller replaces
// the outer decoder's settings. Plain json.Unmarshal inside it meant an
// extra key inside a field value was dropped in silence.
func TestValueRejectsUnknownKeys(t *testing.T) {
	var v Value
	if err := json.Unmarshal([]byte(`{"i":1,"flaot":2}`), &v); err == nil {
		t.Fatal("a misspelled key inside a field value was accepted and dropped")
	}
	if err := json.Unmarshal([]byte(`{"i":1}`), &v); err != nil {
		t.Fatalf("a well-formed value was refused: %v", err)
	}
	if v.T != TypeInt || v.I != 1 {
		t.Fatalf("decoded to %+v", v)
	}
}
