package ingest

import (
	"encoding/json"
	"math"
	"math/rand"
	"testing"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/wire"
)

// sampleBytes is the only thing standing between a buffer and a body the
// store answers 413 to, so it has to over-estimate, never under-estimate.
// It used to be 21 bytes short on the fixed part of every sample and one
// byte short on a float field, so a take could exceed --batch-bytes.
func TestSampleBytesBoundsEveryShapeOfSample(t *testing.T) {
	r := rand.New(rand.NewSource(20260917))
	worst := 0
	var worstBody []byte
	for i := 0; i < 200000; i++ {
		s := randomSample(r)
		body, err := json.Marshal(&s)
		if err != nil {
			continue
		}
		// Plus the comma that separates this sample from the next one.
		actual := len(body) + 1
		if d := actual - sampleBytes(&s); d > worst {
			worst, worstBody = d, body
		}
	}
	if worst > 0 {
		t.Errorf("sampleBytes under-counts by %d bytes; worst case: %s", worst, worstBody)
	}
}

// The extremes the random pass is unlikely to reach on its own.
func TestSampleBytesCoversTheWidestEncodings(t *testing.T) {
	cases := []model.Sample{
		{
			TSMs:    math.MaxInt64,
			KeyHint: "stream\x00999999999",
			Labels:  map[string]string{"host": "a\"b\tc", "dc": "<eu&west>"},
			Fields: map[string]model.Value{
				// The widest float64 literal encoding/json emits.
				"f": model.Float(-0.0000012345678901234567),
				"e": model.Float(-1.2345678901234567e+308),
				"i": model.Int(math.MinInt64),
				"b": model.Bool(false),
				"s": model.String("a\nb\\c\"d\x01e"),
			},
		},
		{TSMs: 1, Fields: map[string]model.Value{"x": model.Int(0)}},
		{TSMs: math.MinInt64, Fields: map[string]model.Value{"x": model.Float(1e300)}},
	}
	for _, s := range cases {
		body, err := json.Marshal(&s)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if got, want := len(body)+1, sampleBytes(&s); got > want {
			t.Errorf("sampleBytes = %d but the encoding is %d: %s", want, got, body)
		}
	}
}

// The batch envelope is part of the body the bound is protecting, and a
// flush spanning many sets pays for one per set.
func TestTakeLockedBoundsTheWholeBody(t *testing.T) {
	const budget = 4096
	s := &Sink{
		cfg:     SinkConfig{BatchSize: 1 << 20, BatchBytes: budget},
		buffers: map[string][]model.Sample{},
	}
	for set := 0; set < 12; set++ {
		name := "set_" + string(rune('a'+set))
		for i := 0; i < 40; i++ {
			s.buffers[name] = append(s.buffers[name], model.Sample{
				TSMs:    1700000000000 + int64(i),
				Labels:  map[string]string{"host": "web-01", "pool": "default"},
				Fields:  map[string]model.Value{"requests": model.Int(int64(i))},
				KeyHint: "abcdef01\x00123456",
			})
			s.pending++
		}
	}
	for round := 0; s.pending > 0; round++ {
		if round > 200 {
			t.Fatal("takeLocked made no progress")
		}
		batches, count, _ := s.takeLocked(s.cfg.BatchBytes)
		if count == 0 {
			t.Fatal("takeLocked took nothing while samples remain")
		}
		body, err := json.Marshal(&wire.WriteRequest{Batches: batches})
		if err != nil {
			t.Fatal(err)
		}
		// One sample is always taken even when it does not fit, so a
		// single-sample body is allowed to overrun; anything else is
		// the estimator being wrong.
		if len(body) > budget && count > 1 {
			t.Fatalf("round %d: body is %d bytes for a %d-byte budget (%d samples)", round, len(body), budget, count)
		}
	}
}

func randomSample(r *rand.Rand) model.Sample {
	s := model.Sample{TSMs: r.Int63() - r.Int63()}
	if r.Intn(2) == 0 {
		s.KeyHint = randomText(r, r.Intn(20))
	}
	if n := r.Intn(5); n > 0 {
		s.Labels = make(map[string]string, n)
		for i := 0; i < n; i++ {
			s.Labels[randomText(r, 1+r.Intn(8))] = randomText(r, 1+r.Intn(12))
		}
	}
	nf := 1 + r.Intn(4)
	s.Fields = make(map[string]model.Value, nf)
	for i := 0; i < nf; i++ {
		name := randomText(r, 1+r.Intn(8))
		switch r.Intn(5) {
		case 0:
			s.Fields[name] = model.Int(r.Int63() - r.Int63())
		case 1:
			f := math.Float64frombits(r.Uint64())
			if !model.IsFinite(f) {
				f = -0.0000012345678901234567
			}
			s.Fields[name] = model.Float(f)
		case 2:
			s.Fields[name] = model.Float(float64(r.Intn(1000)) / 1e9)
		case 3:
			s.Fields[name] = model.String(randomText(r, r.Intn(16)))
		default:
			s.Fields[name] = model.Bool(r.Intn(2) == 0)
		}
	}
	return s
}

// randomText leans on the bytes encoding/json escapes, since those are
// where a raw length measurement went wrong.
func randomText(r *rand.Rand, n int) string {
	const alphabet = "abcXYZ019_.-\"\\\n\t\r<>&\x00\x01\x1f"
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[r.Intn(len(alphabet))]
	}
	return string(b)
}
