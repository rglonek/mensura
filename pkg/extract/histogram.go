package extract

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/rglonek/mensura/pkg/model"
)

// tailField is the column `tail: true` writes: the count beyond the
// declared buckets. The name is part of the spec surface
// (03-extraction.md section 8), so it is a constant both the writer here
// and the compile-time collision check in spec.go can share.
const tailField = "tail"

// expand splits a raw histogram payload into one field per declared
// bucket, then derives the cumulative and tail fields. Doing this at
// ingest keeps the query path a plain scan.
func (b *BucketSet) expand(raw string, fields map[string]model.Value) error {
	counts := map[string]int64{}
	switch b.Parse {
	case "paren_pairs":
		// "(00: 29931197672) (01: 5096257) ..."
		rest := raw
		for {
			open := strings.Index(rest, "(")
			if open < 0 {
				break
			}
			close := strings.Index(rest[open:], ")")
			if close < 0 {
				break
			}
			pair := rest[open+1 : open+close]
			rest = rest[open+close+1:]
			k, v, ok := splitPair(pair)
			if !ok {
				continue
			}
			counts[k] = v
		}
	case "csv":
		for i, part := range strings.Split(raw, ",") {
			v, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
			if err != nil || i >= len(b.Buckets) {
				continue
			}
			counts[b.Buckets[i]] = v
		}
	case "key_value":
		for _, part := range strings.Fields(raw) {
			if k, v, ok := splitPair(part); ok {
				counts[k] = v
			}
		}
	default:
		return fmt.Errorf("extract: bucket set %s: unknown parse mode %q", b.Name, b.Parse)
	}

	// A bucket the payload did not carry is absent, not zero. Writing
	// Int(0) for it drew on a heatmap as a measured zero -- the same
	// invention the "captured no buckets group" guard above exists to
	// prevent, one bucket at a time instead of a whole row.
	var sum int64
	present := 0
	for _, name := range b.Buckets {
		v, ok := counts[name]
		if !ok {
			continue
		}
		fields[name] = model.Int(v)
		sum += v
		present++
	}
	if present == 0 {
		return fmt.Errorf("extract: bucket set %s: the payload carried none of the declared buckets", b.Name)
	}
	if !b.Cumulative && !b.Tail {
		return nil
	}
	if present < len(b.Buckets) {
		// Both derivations read every bucket: a tail is the total less
		// the sum of all of them, and "<bucket>plus" is the count at or
		// above that bucket. With some missing they would be wrong, and a
		// wrong number is worse than an absent one.
		return nil
	}
	total := sum
	if b.TotalField != "" {
		if tv, ok := fields[b.TotalField]; ok {
			if n, ok := tv.AsInt(); ok {
				total = n
			}
		}
	}
	tail := total - sum
	if tail < 0 {
		tail = 0
	}
	if b.Tail {
		fields[tailField] = model.Int(tail)
	}
	if b.Cumulative {
		// <bucket>plus is the count at or above that bucket, which is what
		// a percentile estimate reads.
		running := tail
		for i := len(b.Buckets) - 1; i >= 0; i-- {
			running += counts[b.Buckets[i]]
			fields[b.Buckets[i]+"plus"] = model.Int(running)
		}
	}
	return nil
}

func splitPair(s string) (string, int64, bool) {
	i := strings.IndexAny(s, ":=")
	if i < 0 {
		return "", 0, false
	}
	k := strings.TrimSpace(s[:i])
	v, err := strconv.ParseInt(strings.TrimSpace(s[i+1:]), 10, 64)
	if err != nil {
		return "", 0, false
	}
	return k, v, true
}
