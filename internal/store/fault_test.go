package store

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/wire"
)

// A dictionary record the store could not persist is a fault the store
// owns, not a rejection of the sample that happened to be in hand.
//
// rowFor reports both through one error, and Write used to turn every one
// of them into a named rejection: the ingester was told its batch had
// landed minus a few rows, its checkpoint advanced past them, and the
// records were gone with nothing but a WARNING on its own console. A 5xx
// is what makes the write client retry the batch it still holds.
func TestAPersistenceFaultIsNotASampleRejection(t *testing.T) {
	s := openTestStore(t)
	// The engine is closed underneath the store, which is what a full
	// disk or an I/O fault looks like to PutDict. The store itself is
	// still open, so the write path runs exactly as it would.
	if err := s.DB().Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}
	resp, err := s.Write(&wire.WriteRequest{Batches: []model.Batch{{
		Set: "app",
		Samples: []model.Sample{{
			TSMs:   1756382400000,
			Labels: map[string]string{"host": "a"},
			Fields: map[string]model.Value{"n": model.Int(1)},
		}},
	}}}, "", "")
	if err == nil {
		t.Fatalf("a dictionary write that failed was reported as a per-sample rejection: %+v", resp)
	}
	if !errors.Is(err, ErrStoreFault) {
		t.Fatalf("error %v is not an ErrStoreFault", err)
	}
	// And the HTTP layer must not turn it into a 400: the write client
	// classifies a 4xx as fatal and the sink drops the batch outright.
	var bad *ErrBadRequest
	if errors.As(err, &bad) {
		t.Fatalf("a store fault was classified as a client fault: %v", err)
	}
}

// The same fault over the API surface: a 500, which wire.Client retries,
// rather than a 200 whose body quietly names the sample as refused.
func TestAPersistenceFaultAnswersAServerError(t *testing.T) {
	s := openTestStore(t)
	api := NewAPI(s, APIConfig{})
	if err := s.DB().Close(); err != nil {
		t.Fatalf("close engine: %v", err)
	}
	body := `{"batches":[{"set":"app","samples":[{"ts_ms":1756382400000,"labels":{"host":"a"},"fields":{"n":{"i":1}}}]}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/write", strings.NewReader(body))
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status %d, want 500 (body %s)", w.Code, w.Body.String())
	}
}

// A fault the *client* owns still comes back as a named rejection, with
// the rest of the batch committed: the distinction has to cut both ways.
func TestACardinalityRefusalIsStillARejection(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.Durability = "batch"
	cfg.RetentionSweep = 0
	cfg.MaxLabelCardinality = 1
	s, err := Open(cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = s.Close() }()

	resp, err := s.Write(&wire.WriteRequest{Batches: []model.Batch{{
		Set: "app",
		Samples: []model.Sample{
			{TSMs: 1756382400000, Labels: map[string]string{"host": "a"}, Fields: map[string]model.Value{"n": model.Int(1)}},
			{TSMs: 1756382400001, Labels: map[string]string{"host": "b"}, Fields: map[string]model.Value{"n": model.Int(2)}},
		},
	}}}, "", "")
	if err != nil {
		t.Fatalf("an over-cardinality label failed the whole request: %v", err)
	}
	if resp.Accepted != 1 || resp.Refused() != 1 {
		t.Fatalf("accepted %d refused %d, want 1 and 1", resp.Accepted, resp.Refused())
	}
}
