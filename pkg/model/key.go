package model

import (
	"encoding/binary"
	"encoding/hex"

	"github.com/zeebo/xxh3"
)

// PrimaryKey derives the 16-byte row key for a sample under the given
// scheme. Content keying makes at-least-once delivery behave as
// exactly-once for the common case; offset keying keeps every occurrence
// distinct. See docs/design/04-wire-protocol.md section 6.
func PrimaryKey(set string, s *Sample, scheme KeyScheme) [16]byte {
	h := xxh3.New()
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(s.TSMs))
	_, _ = h.WriteString(set)
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(ts[:])
	for _, k := range s.SortedLabelKeys() {
		_, _ = h.WriteString(k)
		_, _ = h.Write([]byte{'='})
		_, _ = h.WriteString(s.Labels[k])
		_, _ = h.Write([]byte{0})
	}
	if scheme == KeyOffset {
		_, _ = h.WriteString(s.KeyHint)
	} else {
		for _, k := range s.SortedFieldKeys() {
			v := s.Fields[k]
			_, _ = h.WriteString(k)
			_, _ = h.Write([]byte{'='})
			_, _ = h.Write([]byte{byte(v.T)})
			_, _ = h.WriteString(v.String())
			_, _ = h.Write([]byte{0})
		}
	}
	sum := h.Sum128().Bytes()
	return sum
}

// KeyHex is the human-facing form of a primary key: 32 lowercase hex
// characters, which is what the debug endpoints print and accept.
func KeyHex(k [16]byte) string { return hex.EncodeToString(k[:]) }
