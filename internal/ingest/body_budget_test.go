package ingest

import (
	"encoding/json"
	"math/rand"
	"strings"
	"testing"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/wire"
)

// jsonStringBytes has to be an upper bound on what encoding/json emits,
// and it was not: it counted bytes where two of the encoder's rules are
// about runes.
//
// An invalid UTF-8 byte is emitted as � -- six bytes for one -- and
// that is not a pathological input. A `kind: string` field is a whole log
// message, and any log that is not UTF-8 (Latin-1 or CP1252 accented
// text, Windows smart quotes, a cut multi-byte sequence) is full of them.
// U+2028 and U+2029 cost six bytes for three.
func TestJSONStringBytesIsAnUpperBound(t *testing.T) {
	fixed := []string{
		"", "hello", `a"b`, "a<b>c&d", "\x01\x02", "ok\x7f",
		"\xff\xfe", "caf\xe9", "  ", "\xed\xa0\x80",
		"na\xefve \xabquoted\xbb", strings.Repeat("\xe9", 64),
		"mixed \xff   \t <tag> caf\xe9 \\ \" done",
	}
	check := func(s string) {
		b, err := json.Marshal(s)
		if err != nil {
			t.Fatalf("marshal %q: %v", s, err)
		}
		actual := len(b) - 2 // the surrounding quotes are counted by the caller
		if est := jsonStringBytes(s); est < actual {
			t.Fatalf("jsonStringBytes(%q) = %d, but it encodes to %d bytes", s, est, actual)
		}
	}
	for _, s := range fixed {
		check(s)
	}
	// And over random byte strings, because the interesting inputs are
	// the ones nobody writes down.
	rng := rand.New(rand.NewSource(20260930))
	buf := make([]byte, 48)
	for i := 0; i < 20000; i++ {
		n := rng.Intn(len(buf) + 1)
		for j := 0; j < n; j++ {
			buf[j] = byte(rng.Intn(256))
		}
		check(string(buf[:n]))
	}
	// Including strings built out of the runes the encoder treats
	// specially, so the multi-byte arms are exercised deliberately.
	special := []rune{' ', ' ', '�', 'é', '漢', '𝄞', '<', '&', '"', '\n', 0x01}
	var sb strings.Builder
	for i := 0; i < 2000; i++ {
		sb.Reset()
		for j := 0; j < rng.Intn(12); j++ {
			sb.WriteRune(special[rng.Intn(len(special))])
		}
		check(sb.String())
	}
}

// The end of the same problem: a take that exceeds BatchBytes presents a
// body past the store's max_request_bytes, which comes back 413 -- a
// status wire.Client classifies as fatal, so the sink drops the whole
// batch, reports it to the delivery observers as a hole, and every
// followed file's checkpoint freezes behind it. That is the exact failure
// BatchBytes exists to prevent.
func TestTakeLockedStaysInsideBatchBytes(t *testing.T) {
	const budget = 64 << 10
	cases := map[string]string{
		"latin-1 message":     strings.Repeat("\xe9", 500),
		"line separators":     strings.Repeat(" ", 200),
		"quotes and controls": strings.Repeat("\"\x01<", 200),
		"plain":               strings.Repeat("a", 500),
	}
	for name, msg := range cases {
		s := &Sink{
			cfg:      SinkConfig{BatchSize: 100_000, BatchBytes: budget, MaxBufferedSamples: 100_000},
			buffers:  map[string][]model.Sample{},
			metaSent: map[string]struct{}{},
		}
		for i := 0; i < 2000; i++ {
			s.buffers["app"] = append(s.buffers["app"], model.Sample{
				TSMs:    int64(i + 1),
				Labels:  map[string]string{"host": msg[:20]},
				Fields:  map[string]model.Value{"m": model.String(msg)},
				KeyHint: "stream\x00" + msg[:8],
			})
			s.pending++
		}
		batches, n, _ := s.takeLocked(s.sampleBudget(nil, nil))
		if n == 0 {
			t.Fatalf("%s: took nothing", name)
		}
		body, err := json.Marshal(&wire.WriteRequest{Batches: batches})
		if err != nil {
			t.Fatalf("%s: marshal: %v", name, err)
		}
		// The outer envelope -- {"batches":[ ... ]} -- is a fixed handful
		// of bytes that takeLocked does not charge for, so the bound is
		// the budget plus that.
		if len(body) > budget+64 {
			t.Errorf("%s: took %d sample(s) into a %d-byte body against a %d-byte budget (%.1fx)",
				name, n, len(body), budget, float64(len(body))/float64(budget))
		}
	}
}
