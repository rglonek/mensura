package model

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// ReservedPrefix marks sets, fields and label keys that belong to the
// store itself. Clients may not create them, with the single documented
// exception of the ingest-progress set.
const ReservedPrefix = "_mensura"

// IngestSet is the one reserved set clients may write, and only for
// samples carrying their own client label.
const IngestSet = "_mensura_ingest"

const (
	CatalogueSet = "_mensura_catalogue"
	LabelsSet    = "_mensura_labels"
)

const maxIdentLen = 128
const maxLabelValueLen = 1024

func validIdent(s string, allowLeadingDigit bool) bool {
	if s == "" || len(s) > maxIdentLen {
		return false
	}
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
		case i > 0 && (r >= '0' && r <= '9' || r == '.' || r == '-'):
		case i == 0 && allowLeadingDigit && r >= '0' && r <= '9':
		default:
			return false
		}
	}
	return true
}

// ValidateSetName enforces the set charset. '@' is excluded because the
// store appends shard suffixes with it.
func ValidateSetName(s string) error {
	if !validIdent(s, false) {
		return fmt.Errorf("invalid set name %q: expected [A-Za-z_][A-Za-z0-9_.-]{0,127}", s)
	}
	return nil
}

// ValidateFieldName is deliberately looser than the set and label rules:
// a field may start with a digit, because histogram bucket columns are
// conventionally named "00".."23" (and "03plus" for the cumulative form).
// Such a name is not a bare MQL identifier, so a query quotes it:
// SELECT "00". Set names and label keys stay strict.
func ValidateFieldName(s string) error {
	if !validIdent(s, true) {
		return fmt.Errorf("invalid field name %q: expected [A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", s)
	}
	if s == TimestampField {
		return fmt.Errorf("field name %q is reserved for the indexed timestamp column", s)
	}
	return nil
}

func ValidateLabelKey(s string) error {
	if !validIdent(s, false) {
		return fmt.Errorf("invalid label key %q: expected [A-Za-z_][A-Za-z0-9_.-]{0,127}", s)
	}
	// Labels and fields share one column namespace on a row, so a label
	// named "timestamp" would overwrite the indexed column with a
	// dictionary index and put the row at a time nothing can find.
	if s == TimestampField {
		return fmt.Errorf("label key %q is reserved for the indexed timestamp column", s)
	}
	return nil
}

func ValidateLabelValue(s string) error {
	if s == "" {
		return fmt.Errorf("label value must not be empty")
	}
	if len(s) > maxLabelValueLen {
		return fmt.Errorf("label value exceeds %d bytes", maxLabelValueLen)
	}
	if !utf8.ValidString(s) {
		return fmt.Errorf("label value is not valid UTF-8")
	}
	return nil
}

// IsReserved reports whether a name belongs to the store's own namespace.
func IsReserved(name string) bool { return strings.HasPrefix(name, ReservedPrefix) }
