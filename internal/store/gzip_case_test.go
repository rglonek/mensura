package store

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/wire"
)

// A content coding is a case-insensitive token (RFC 9110 section 8.4.1),
// and the body reader matched it exactly against "gzip": a client sending
// `Content-Encoding: GZIP` had its deflate bytes handed straight to the
// JSON decoder and was answered 400 about malformed JSON for a request
// that was entirely well formed.
func TestGzipContentEncodingIsCaseInsensitive(t *testing.T) {
	s := openTestStore(t)
	api := NewAPI(s, APIConfig{})
	body, err := json.Marshal(&wire.WriteRequest{Batches: []model.Batch{{
		Set: "app", Samples: []model.Sample{{TSMs: base(), Fields: map[string]model.Value{"v": model.Int(1)}}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, encoding := range []string{"gzip", "GZIP", "Gzip", " gzip "} {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		if _, err := zw.Write(body); err != nil {
			t.Fatal(err)
		}
		if err := zw.Close(); err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/v1/write", strings.NewReader(buf.String()))
		req.Header.Set("Content-Encoding", encoding)
		w := httptest.NewRecorder()
		api.Handler().ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("Content-Encoding %q answered %d: %s", encoding, w.Code, w.Body.String())
		}
	}
}
