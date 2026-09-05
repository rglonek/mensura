package engine

import "encoding/binary"

// Key prefixes. One byte each, so a prefix scan is a two-byte bound.
const (
	prefixMeta  = 'M' // schemas, set-name/id maps, storage version
	prefixData  = 'D' // unindexed rows, or the forward pointer of an indexed row
	prefixIndex = 'I' // indexed rows, payload clustered at the index key
	prefixDict  = 'L' // label dictionaries
)

// biasInt flips the sign bit so that big-endian byte order matches numeric
// order for signed values.
func biasInt(v int64) uint64 { return uint64(v) ^ (1 << 63) }

func unbiasInt(u uint64) int64 { return int64(u ^ (1 << 63)) }

func be4(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

// dataKey is D | be4(setID) | pk.
func dataKey(setID uint32, pk [16]byte) []byte {
	k := make([]byte, 0, 1+4+16)
	k = append(k, prefixData)
	k = append(k, be4(setID)...)
	k = append(k, pk[:]...)
	return k
}

func dataPrefix(setID uint32) []byte {
	k := make([]byte, 0, 5)
	k = append(k, prefixData)
	return append(k, be4(setID)...)
}

// indexKey is I | be4(setID) | be4(colID) | be8(biased value) | pk.
func indexKey(setID, colID uint32, val int64, pk [16]byte) []byte {
	k := make([]byte, 0, 1+4+4+8+16)
	k = append(k, prefixIndex)
	k = append(k, be4(setID)...)
	k = append(k, be4(colID)...)
	var v [8]byte
	binary.BigEndian.PutUint64(v[:], biasInt(val))
	k = append(k, v[:]...)
	return append(k, pk[:]...)
}

func indexPrefix(setID, colID uint32) []byte {
	k := make([]byte, 0, 9)
	k = append(k, prefixIndex)
	k = append(k, be4(setID)...)
	return append(k, be4(colID)...)
}

// indexBound builds the lower or upper bound of a value range. The upper
// bound is exclusive, so callers pass hi+1 to include hi.
func indexBound(setID, colID uint32, val int64) []byte {
	k := indexPrefix(setID, colID)
	var v [8]byte
	binary.BigEndian.PutUint64(v[:], biasInt(val))
	return append(k, v[:]...)
}

// indexKeyValue extracts the indexed value back out of an index key.
func indexKeyValue(key []byte) (int64, bool) {
	if len(key) < 1+4+4+8 {
		return 0, false
	}
	return unbiasInt(binary.BigEndian.Uint64(key[9:17])), true
}

func indexKeyPK(key []byte) ([16]byte, bool) {
	var pk [16]byte
	if len(key) != 1+4+4+8+16 {
		return pk, false
	}
	copy(pk[:], key[17:])
	return pk, true
}

// dataPointerTag marks a D/ value as a forward pointer to an index key
// rather than a row payload.
//
// The pointer used to be a bare 8 bytes and was recognised by its length,
// but PutBatch also stores a whole row under D/ when that row carries no
// indexed column -- and a small row encodes to exactly 8 bytes (a one
// column, three-character name, one byte value). Length alone therefore
// could not tell a pointer from a row. The tag can: encodeRow writes a
// uvarint column count first, and a 9-byte payload whose count is zero is
// not something it can produce.
const dataPointerTag byte = 0x00

// dataPointer encodes the forward pointer stored under a D/ key for an
// indexed row.
func dataPointer(val int64) []byte {
	b := make([]byte, 9)
	b[0] = dataPointerTag
	binary.BigEndian.PutUint64(b[1:], biasInt(val))
	return b
}

// readDataPointer recovers the indexed value from a D/ payload, reporting
// whether it was one. The bare 8-byte form written by an earlier build is
// still recognised, so a data directory does not have to be rewritten.
func readDataPointer(payload []byte) (int64, bool) {
	if len(payload) == 9 && payload[0] == dataPointerTag {
		return unbiasInt(binary.BigEndian.Uint64(payload[1:])), true
	}
	if len(payload) == 8 {
		return unbiasInt(binary.BigEndian.Uint64(payload)), true
	}
	return 0, false
}

// taggedDataPointer reports whether a D/ payload is the tagged forward
// pointer this build writes, as opposed to the untagged eight-byte form an
// earlier one did. The distinction matters where the index key a pointer
// names is missing: a tagged payload is a pointer and nothing else, while
// an untagged one may still be a small row.
func taggedDataPointer(payload []byte) bool {
	return len(payload) == 9 && payload[0] == dataPointerTag
}

// prefixEnd returns the exclusive upper bound covering every key that
// starts with prefix.
func prefixEnd(prefix []byte) []byte {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	for i := len(end) - 1; i >= 0; i-- {
		if end[i] < 0xff {
			end[i]++
			return end[:i+1]
		}
	}
	return nil // prefix is all 0xff: no upper bound
}

func metaKey(parts ...string) []byte {
	k := []byte{prefixMeta}
	for i, p := range parts {
		if i > 0 {
			k = append(k, '/')
		}
		k = append(k, p...)
	}
	return k
}

func dictKey(name string) []byte {
	return append([]byte{prefixDict}, name...)
}
