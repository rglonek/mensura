package engine

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/rglonek/mensura/pkg/model"
)

// Row is one sparse record: only the columns the sample actually carried.
type Row map[string]model.Value

// Row payload wire format (v1), a TLV with a jump-skip directory:
//
//	uvarint  column count
//	repeated:
//	  uvarint nameLen | name bytes
//	  byte    type tag
//	  uvarint valueLen | value bytes
//
// The per-value length prefix is the jump: decoding a projection of k
// columns out of n costs O(n) varint decodes and k value decodes, never a
// full scan of the payload bytes.

var errCorruptRow = errors.New("engine: corrupt row payload")

func encodeRow(r Row) []byte {
	buf := make([]byte, 0, 32*len(r)+8)
	buf = binary.AppendUvarint(buf, uint64(len(r)))
	for name, v := range r {
		buf = binary.AppendUvarint(buf, uint64(len(name)))
		buf = append(buf, name...)
		buf = append(buf, byte(v.T))
		switch v.T {
		case model.TypeInt:
			buf = binary.AppendUvarint(buf, 8)
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], uint64(v.I))
			buf = append(buf, b[:]...)
		case model.TypeFloat:
			buf = binary.AppendUvarint(buf, 8)
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], math.Float64bits(v.F))
			buf = append(buf, b[:]...)
		case model.TypeString:
			buf = binary.AppendUvarint(buf, uint64(len(v.S)))
			buf = append(buf, v.S...)
		case model.TypeBool:
			buf = binary.AppendUvarint(buf, 1)
			if v.B {
				buf = append(buf, 1)
			} else {
				buf = append(buf, 0)
			}
		default:
			buf = binary.AppendUvarint(buf, 0)
		}
	}
	return buf
}

func decodeValue(t model.ValueType, b []byte) (model.Value, error) {
	switch t {
	case model.TypeInt:
		if len(b) != 8 {
			return model.Value{}, errCorruptRow
		}
		return model.Int(int64(binary.BigEndian.Uint64(b))), nil
	case model.TypeFloat:
		if len(b) != 8 {
			return model.Value{}, errCorruptRow
		}
		return model.Float(math.Float64frombits(binary.BigEndian.Uint64(b))), nil
	case model.TypeString:
		return model.String(string(b)), nil
	case model.TypeBool:
		if len(b) != 1 {
			return model.Value{}, errCorruptRow
		}
		return model.Bool(b[0] == 1), nil
	}
	return model.Value{}, errCorruptRow
}

// walkRow visits every column without decoding values. The callback
// receives the column name and the raw value slice; returning false stops
// the walk early.
func walkRow(payload []byte, fn func(name string, t model.ValueType, raw []byte) bool) error {
	p := payload
	n, adv := binary.Uvarint(p)
	if adv <= 0 {
		return errCorruptRow
	}
	p = p[adv:]
	for i := uint64(0); i < n; i++ {
		nameLen, adv := binary.Uvarint(p)
		if adv <= 0 || uint64(len(p[adv:])) < nameLen {
			return errCorruptRow
		}
		p = p[adv:]
		name := string(p[:nameLen])
		p = p[nameLen:]
		if len(p) < 1 {
			return errCorruptRow
		}
		t := model.ValueType(p[0])
		p = p[1:]
		valLen, adv := binary.Uvarint(p)
		if adv <= 0 || uint64(len(p[adv:])) < valLen {
			return errCorruptRow
		}
		p = p[adv:]
		raw := p[:valLen]
		p = p[valLen:]
		if !fn(name, t, raw) {
			return nil
		}
	}
	return nil
}

// decodeRow materialises a row. With a non-nil projection only the named
// columns are decoded; the rest are skipped by their length prefix.
func decodeRow(payload []byte, projection map[string]struct{}) (Row, error) {
	out := Row{}
	var bad error
	err := walkRow(payload, func(name string, t model.ValueType, raw []byte) bool {
		if projection != nil {
			if _, want := projection[name]; !want {
				return true
			}
		}
		v, err := decodeValue(t, raw)
		if err != nil {
			// A value that will not decode is reported, not dropped.
			// Skipping it made the column read as absent, so the same
			// corruption surfaced as an error when a pushdown filter
			// touched the column (lazyRow.get records it) and as a
			// silently missing field when nothing did.
			bad = err
			return false
		}
		out[name] = v
		return true
	})
	if err != nil {
		return nil, err
	}
	if bad != nil {
		return nil, bad
	}
	return out, nil
}

// lazyRow answers column lookups straight off the payload bytes, so a
// pushdown filter only pays for the columns it actually references.
type lazyRow struct {
	payload []byte
	cache   map[string]model.Value
	err     error
}

func newLazyRow(payload []byte) *lazyRow {
	return &lazyRow{payload: payload, cache: make(map[string]model.Value, 4)}
}

func (l *lazyRow) get(name string) (model.Value, bool) {
	if v, ok := l.cache[name]; ok {
		return v, v.Valid()
	}
	var found model.Value
	var bad error
	if err := walkRow(l.payload, func(n string, t model.ValueType, raw []byte) bool {
		if n != name {
			return true
		}
		v, err := decodeValue(t, raw)
		if err != nil {
			// Recorded, not swallowed. Leaving found invalid made the
			// column read as absent, so a pushdown filter quietly
			// excluded the row instead of letting the scan report the
			// corruption -- the same asymmetry decodeRow was changed to
			// remove.
			bad = err
			return false
		}
		found = v
		return false
	}); err != nil {
		// A corrupt payload must not read as "the column is absent": a
		// pushdown filter would then quietly exclude the row instead of
		// letting the scan report the corruption.
		l.err = err
	} else if bad != nil {
		l.err = bad
	}
	l.cache[name] = found
	return found, found.Valid()
}
