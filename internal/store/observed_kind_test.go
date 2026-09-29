package store

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/wire"
)

// TestObservedFieldKindIsNotADeclaration pins the difference between a
// field the catalogue merely saw and one an ingester declared.
//
// observeSet used to stamp KindGauge onto a field it created from a
// sample, which made the two indistinguishable to applyFieldMeta's
// conflict test: a set written before its metadata arrived -- a second
// ingester, a second profile, or simply a spec with no `fields:` entry for
// that column on the first batch -- recorded a CatalogueConflict
// "was gauge, now counter" for a declaration nothing had disagreed with,
// and /v1/catalogue reported it to every client for the life of the store.
func TestObservedFieldKindIsNotADeclaration(t *testing.T) {
	s := openTestStore(t)

	// A sample with no metadata behind it: the field is observed only.
	writeSamples(t, s, "app", []model.Sample{{
		TSMs:   1_700_000_000_000,
		Labels: map[string]string{"host": "a"},
		Fields: map[string]model.Value{"tx": model.Int(1)},
	}})

	cat := s.Catalogue()
	if len(cat.Conflicts) != 0 {
		t.Fatalf("an observed field recorded a conflict: %+v", cat.Conflicts)
	}
	// The rendered shape does not change: an undeclared field still reads
	// as a gauge, which is what the query builder and the validator expect.
	if got := cat.Sets[0].Fields["tx"].Kind; got != model.KindGauge {
		t.Fatalf("observed field reported kind %q, want %q", got, model.KindGauge)
	}
	if info, ok := s.Schema().Field("app", "tx"); !ok || info.Kind != model.KindGauge {
		t.Fatalf("Schema.Field reported %+v, ok=%v", info, ok)
	}

	// Now the declaration arrives. It is the first thing anyone has said
	// about this field's kind, so it is not a disagreement.
	if _, err := s.Write(&wire.WriteRequest{
		FieldMeta: []wire.FieldMeta{{Set: "app", Field: "tx", Kind: model.KindCounter}},
	}, "", "test"); err != nil {
		t.Fatalf("declare: %v", err)
	}
	cat = s.Catalogue()
	if len(cat.Conflicts) != 0 {
		t.Fatalf("declaring the kind of an observed field recorded a conflict: %+v", cat.Conflicts)
	}
	if got := cat.Sets[0].Fields["tx"].Kind; got != model.KindCounter {
		t.Fatalf("after the declaration the kind is %q, want %q", got, model.KindCounter)
	}

	// Two ingesters that really do disagree are still recorded.
	if _, err := s.Write(&wire.WriteRequest{
		FieldMeta: []wire.FieldMeta{{Set: "app", Field: "tx", Kind: model.KindDelta}},
	}, "", "test"); err != nil {
		t.Fatalf("redeclare: %v", err)
	}
	cat = s.Catalogue()
	if len(cat.Conflicts) != 1 {
		t.Fatalf("a real disagreement was not recorded: %+v", cat.Conflicts)
	}
	if cat.Conflicts[0].Was != string(model.KindCounter) || cat.Conflicts[0].Now != string(model.KindDelta) {
		t.Fatalf("conflict reads %+v", cat.Conflicts[0])
	}
}

// TestWriteDuringShutdownAsksTheClientBack pins the status the write API
// owes a batch that arrives while the store is closing.
//
// handleQuery and writeAdminErr both answer ErrClosed with 503 and a
// Retry-After; handleWrite answered 500, which wire.Client retries on its
// own exponential backoff against a process that has already gone.
func TestWriteDuringShutdownAsksTheClientBack(t *testing.T) {
	s := openTestStore(t)
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	api := NewAPI(s, APIConfig{})
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()

	body := `{"batches":[{"set":"app","samples":[{"ts_ms":1700000000000,"fields":{"tx":{"i":1}}}]}]}`
	resp, err := http.Post(srv.URL+"/v1/write", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("a write to a closing store answered %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}
	if got := resp.Header.Get("Retry-After"); got == "" {
		t.Fatal("no Retry-After on the shed write")
	}
	// Queries answer the same way, which is the agreement being pinned.
	qresp, err := http.Post(srv.URL+"/v1/query", "application/json", strings.NewReader(`{"ast":{"from":"app","select":[{"field":"tx"}]}}`))
	if err != nil {
		t.Fatalf("query post: %v", err)
	}
	defer qresp.Body.Close()
	if qresp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("a query to a closing store answered %d, want %d", qresp.StatusCode, http.StatusServiceUnavailable)
	}
}
