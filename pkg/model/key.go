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
//
// Every variable-length component is length-prefixed rather than
// separated by a delimiter byte. Delimiters were ambiguous: a label value
// is any valid UTF-8, so it may contain the NUL that terminated it and
// the '=' that separated it from its key. That made
//
//	{a: "b", c: "d"}   and   {a: "b\x00c=d"}
//
// hash to the same 16 bytes, and PutBatch assumes a key is either new or
// carries the same indexed value, so the second sample silently
// overwrote the first while the write API counted both as accepted. A
// length prefix cannot be forged from content, so distinct inputs now
// produce distinct byte streams.
func PrimaryKey(set string, s *Sample, scheme KeyScheme) [16]byte {
	h := xxh3.New()
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(s.TSMs))
	hashPart(h, set)
	_, _ = h.Write(ts[:])
	labels := s.SortedLabelKeys()
	hashLen(h, len(labels))
	for _, k := range labels {
		hashPart(h, k)
		hashPart(h, s.Labels[k])
	}
	if scheme == KeyOffset {
		hashPart(h, s.KeyHint)
	} else {
		fields := s.SortedFieldKeys()
		hashLen(h, len(fields))
		for _, k := range fields {
			v := s.Fields[k]
			hashPart(h, k)
			_, _ = h.Write([]byte{byte(v.T)})
			hashPart(h, v.String())
		}
	}
	sum := h.Sum128().Bytes()
	return sum
}

// hashPart writes one length-prefixed component.
func hashPart(h *xxh3.Hasher, s string) {
	hashLen(h, len(s))
	_, _ = h.WriteString(s)
}

func hashLen(h *xxh3.Hasher, n int) {
	var buf [binary.MaxVarintLen64]byte
	_, _ = h.Write(buf[:binary.PutUvarint(buf[:], uint64(n))])
}

// KeyHex is the human-facing form of a primary key: 32 lowercase hex
// characters, which is what the debug endpoints print and accept.
func KeyHex(k [16]byte) string { return hex.EncodeToString(k[:]) }
