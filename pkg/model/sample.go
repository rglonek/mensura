package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

// TimestampField is the reserved, indexed column present on every metric
// row. Specs may not produce a field with this name.
const TimestampField = "timestamp"

// KeyScheme selects how a row's primary key is derived. See
// docs/design/04-wire-protocol.md section 6.
type KeyScheme string

const (
	// KeyContent hashes the whole sample, so re-ingesting identical data
	// is a no-op and two identical records in the same millisecond from
	// the same stream collapse into one row.
	KeyContent KeyScheme = "content"
	// KeyOffset hashes the caller-supplied hint (stream identity plus
	// byte offset), so every occurrence is a distinct row.
	KeyOffset KeyScheme = "offset"
)

// Sample is one extracted measurement: a timestamp, the labels that
// identify its stream, and the fields it carries.
type Sample struct {
	TSMs    int64             `json:"ts_ms"`
	Labels  map[string]string `json:"labels,omitempty"`
	Fields  map[string]Value  `json:"fields"`
	KeyHint string            `json:"key_hint,omitempty"`
}

// Batch is a set-scoped group of samples; the unit of transfer and of
// commit.
type Batch struct {
	Set     string   `json:"set"`
	Samples []Sample `json:"samples"`
}

var ErrNoFields = errors.New("sample carries no fields")

// Validate checks a sample against the identifier and timestamp rules
// before it can reach the engine.
func (s *Sample) Validate() error {
	if s.TSMs <= 0 {
		return errors.New("sample timestamp must be a positive epoch-millisecond value")
	}
	if len(s.Fields) == 0 {
		return ErrNoFields
	}
	for k, v := range s.Labels {
		if err := ValidateLabelKey(k); err != nil {
			return err
		}
		// The value is checked here too, not only where it is interned,
		// so a bad label is named as a rejection rather than surfacing
		// later as an opaque write failure.
		if err := ValidateLabelValue(v); err != nil {
			return fmt.Errorf("label %q: %w", k, err)
		}
	}
	for k, v := range s.Fields {
		if err := ValidateFieldName(k); err != nil {
			return err
		}
		if !v.Valid() {
			return errors.New("field " + k + " has no value")
		}
	}
	return nil
}

// SortedLabelKeys returns the label keys in a stable order. Canonical
// ordering is what makes the content key reproducible across processes.
func (s *Sample) SortedLabelKeys() []string {
	keys := make([]string, 0, len(s.Labels))
	for k := range s.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// SortedFieldKeys returns the field names in a stable order.
func (s *Sample) SortedFieldKeys() []string {
	keys := make([]string, 0, len(s.Fields))
	for k := range s.Fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// MarshalJSON encodes Value as a tagged union so the wire form stays
// self-describing and a decoder cannot silently retype a field.
func (v Value) MarshalJSON() ([]byte, error) {
	switch v.T {
	case TypeInt:
		return json.Marshal(map[string]any{"i": v.I})
	case TypeFloat:
		return json.Marshal(map[string]any{"f": v.F})
	case TypeString:
		return json.Marshal(map[string]any{"s": v.S})
	case TypeBool:
		return json.Marshal(map[string]any{"b": v.B})
	}
	return nil, errors.New("cannot encode an invalid value")
}

func (v *Value) UnmarshalJSON(b []byte) error {
	var raw struct {
		I *int64   `json:"i"`
		F *float64 `json:"f"`
		S *string  `json:"s"`
		B *bool    `json:"b"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	set := 0
	if raw.I != nil {
		*v, set = Int(*raw.I), set+1
	}
	if raw.F != nil {
		*v, set = Float(*raw.F), set+1
	}
	if raw.S != nil {
		*v, set = String(*raw.S), set+1
	}
	if raw.B != nil {
		*v, set = Bool(*raw.B), set+1
	}
	if set != 1 {
		return errors.New("value: set exactly one of i|f|s|b")
	}
	return nil
}
