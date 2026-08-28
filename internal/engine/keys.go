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
