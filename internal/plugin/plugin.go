// Package plugin is the Grafana backend datasource. It is served by the
// same binary as the store: in embedded mode it calls the query engine
// directly, and in proxy mode it forwards over the query API. Both paths
// go through one QueryService interface, so there is only ever one query
// implementation. See docs/design/08-plugin.md.
package plugin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/rglonek/mensura/internal/store"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

// QueryService is the seam between the plugin and the store. The local
// implementation is a function call; the remote one is an HTTP request.
type QueryService interface {
	Query(ctx context.Context, req *wire.QueryRequest) (*wire.QueryResponse, error)
	Catalogue(ctx context.Context) (wire.Catalogue, error)
	LabelValues(ctx context.Context, key string) ([]string, error)
	Hello(ctx context.Context) (wire.Hello, error)
	Parse(ctx context.Context, text string) (*mql.Query, []mql.Diag, error)
}

// Datasource implements the Grafana backend handlers.
type Datasource struct {
	svc QueryService
}

// ServeLocal serves Grafana from an in-process store: a panel query is a
// range seek, not a network hop.
func ServeLocal(s *store.Store) error {
	return serve(&Datasource{svc: &localService{store: s}})
}

// ServeRemote serves Grafana by forwarding to a store that owns the data
// directory, for deployments where the store must outlive Grafana.
func ServeRemote(c *wire.Client) error {
	return serve(&Datasource{svc: &remoteService{client: c}})
}

func serve(ds *Datasource) error {
	return backend.Serve(backend.ServeOpts{
		QueryDataHandler:    backend.QueryDataHandlerFunc(ds.QueryData),
		CheckHealthHandler:  backend.CheckHealthHandlerFunc(ds.CheckHealth),
		CallResourceHandler: backend.CallResourceHandlerFunc(ds.CallResource),
	})
}

// queryModel is what a panel stores: the AST, never the text, so a grammar
// change cannot break a saved dashboard.
type queryModel struct {
	AST *mql.Query `json:"ast"`
	// Text is kept only so the editor can round-trip what the author typed;
	// it is ignored when an AST is present.
	Text                string `json:"text,omitempty"`
	DisableSeriesSafety bool   `json:"disableSeriesSafety,omitempty"`
	DisableSizeSafety   bool   `json:"disableSizeSafety,omitempty"`
}

func (d *Datasource) QueryData(ctx context.Context, req *backend.QueryDataRequest) (*backend.QueryDataResponse, error) {
	resp := backend.NewQueryDataResponse()
	fromAlert := req.Headers["FromAlert"] == "true"
	for _, q := range req.Queries {
		resp.Responses[q.RefID] = d.runOne(ctx, q, fromAlert)
	}
	return resp, nil
}

func (d *Datasource) runOne(ctx context.Context, q backend.DataQuery, fromAlert bool) backend.DataResponse {
	var model queryModel
	if err := json.Unmarshal(q.JSON, &model); err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, fmt.Sprintf("query model: %v", err))
	}
	ast := model.AST
	if ast == nil {
		if model.Text == "" {
			return backend.DataResponse{}
		}
		parsed, _, err := d.svc.Parse(ctx, model.Text)
		if err != nil {
			return backend.ErrDataResponse(backend.StatusBadRequest, err.Error())
		}
		ast = parsed
	}

	wreq := &wire.QueryRequest{
		AST:        ast,
		FromMs:     q.TimeRange.From.UnixMilli(),
		ToMs:       q.TimeRange.To.UnixMilli(),
		MaxPoints:  int(q.MaxDataPoints),
		IntervalMs: q.Interval.Milliseconds(),
		Options: wire.QueryOptions{
			DisableSeriesSafety: model.DisableSeriesSafety,
			DisableSizeSafety:   model.DisableSizeSafety,
			FromAlert:           fromAlert,
		},
	}
	out, err := d.svc.Query(ctx, wreq)
	if err != nil {
		return backend.ErrDataResponse(backend.StatusBadRequest, err.Error())
	}

	dr := backend.DataResponse{Frames: toFrames(out, mql.Print(ast))}
	for _, w := range out.Warnings {
		dr.Frames = append(dr.Frames, noticeFrame(w))
	}
	if out.Error != "" {
		// Partial results plus the reason: an empty panel with a red banner
		// tells the operator less than a partial shape does.
		dr.Error = fmt.Errorf("%s", out.Error)
		dr.Status = backend.StatusOK
	}
	return dr
}

// toFrames builds one frame per series. Never a wide frame: windows are
// anchored per series, so two series do not share a time axis and merging
// them would require interpolation.
func toFrames(resp *wire.QueryResponse, executed string) data.Frames {
	frames := make(data.Frames, 0, len(resp.Series)+1)
	for _, s := range resp.Series {
		// The arrays travel separately on the wire; in proxy mode they
		// come from another process, so their lengths are checked rather
		// than assumed.
		n := len(s.TSMs)
		if len(s.Values) < n {
			n = len(s.Values)
		}
		times := make([]time.Time, n)
		values := make([]*float64, n)
		for i := 0; i < n; i++ {
			times[i] = time.UnixMilli(s.TSMs[i])
			if i < len(s.IsNull) && s.IsNull[i] {
				continue // a nil value is the connect-break
			}
			v := s.Values[i]
			values[i] = &v
		}
		timeField := data.NewField("time", nil, times)
		valueField := data.NewField("value", labelsOf(s.Labels), values)
		valueField.Config = &data.FieldConfig{
			DisplayNameFromDS: s.Name,
			// Absence of data must not be drawn as a line through it.
			Custom: map[string]any{"spanNulls": false},
		}
		if s.Meta != nil && s.Meta.UnitHint != "" {
			valueField.Config.Unit = s.Meta.UnitHint
		}
		frame := data.NewFrame(s.Name, timeField, valueField)
		frame.Meta = &data.FrameMeta{ExecutedQueryString: executed}
		if len(s.BucketEdges) > 0 {
			frame.Meta.Custom = map[string]any{"bucketEdges": s.BucketEdges}
		}
		frames = append(frames, frame)
	}
	if len(resp.Rows) > 0 {
		frames = append(frames, tableFrame(resp, executed))
	}
	return frames
}

func tableFrame(resp *wire.QueryResponse, executed string) *data.Frame {
	fields := make([]*data.Field, 0, len(resp.Columns))
	for ci, col := range resp.Columns {
		switch col.Type {
		case "time":
			vals := make([]time.Time, len(resp.Rows))
			for ri, r := range resp.Rows {
				if ci < len(r.Values) {
					if ms, ok := toInt64(r.Values[ci]); ok {
						vals[ri] = time.UnixMilli(ms)
					}
				}
			}
			fields = append(fields, data.NewField(col.Name, nil, vals))
		case "number":
			vals := make([]*float64, len(resp.Rows))
			for ri, r := range resp.Rows {
				if ci < len(r.Values) {
					if f, ok := r.Values[ci].(float64); ok {
						v := f
						vals[ri] = &v
					}
				}
			}
			fields = append(fields, data.NewField(col.Name, nil, vals))
		default:
			vals := make([]string, len(resp.Rows))
			for ri, r := range resp.Rows {
				if ci < len(r.Values) {
					if s, ok := r.Values[ci].(string); ok {
						vals[ri] = s
					}
				}
			}
			fields = append(fields, data.NewField(col.Name, nil, vals))
		}
	}
	f := data.NewFrame("table", fields...)
	f.Meta = &data.FrameMeta{ExecutedQueryString: executed}
	return f
}

func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case float64:
		return int64(n), true
	}
	return 0, false
}

func labelsOf(m map[string]string) data.Labels {
	if len(m) == 0 {
		return nil
	}
	out := data.Labels{}
	for k, v := range m {
		out[k] = v
	}
	return out
}

// noticeFrame carries a diagnostic to the panel without failing the query.
func noticeFrame(d mql.Diag) *data.Frame {
	f := data.NewFrame("")
	sev := data.NoticeSeverityWarning
	f.AppendNotices(data.Notice{Severity: sev, Text: d.Code + ": " + d.Msg})
	return f
}

// CheckHealth answers "is there data, and from when to when" rather than a
// bare OK, because that is the question an operator is actually asking.
func (d *Datasource) CheckHealth(ctx context.Context, _ *backend.CheckHealthRequest) (*backend.CheckHealthResult, error) {
	hello, err := d.svc.Hello(ctx)
	if err != nil {
		return &backend.CheckHealthResult{Status: backend.HealthStatusError, Message: err.Error()}, nil
	}
	cat, err := d.svc.Catalogue(ctx)
	if err != nil {
		return &backend.CheckHealthResult{Status: backend.HealthStatusError, Message: err.Error()}, nil
	}
	var first, last int64
	for _, s := range cat.Sets {
		if s.FirstTSMs > 0 && (first == 0 || s.FirstTSMs < first) {
			first = s.FirstTSMs
		}
		if s.LastTSMs > last {
			last = s.LastTSMs
		}
	}
	msg := fmt.Sprintf("mensura-store %s (%s mode, protocol v%d): %d set(s)",
		hello.Version, hello.Mode, hello.Protocol, len(cat.Sets))
	switch {
	case last > 0 && first > 0:
		msg += fmt.Sprintf(", data from %s to %s",
			time.UnixMilli(first).UTC().Format(time.RFC3339),
			time.UnixMilli(last).UTC().Format(time.RFC3339))
	case last > 0:
		// Sets that carry a last timestamp but no first one leave the
		// minimum at zero, which printed as "data from 1970-01-01" --
		// a date nothing in the store has ever held.
		msg += fmt.Sprintf(", data up to %s (no start recorded)",
			time.UnixMilli(last).UTC().Format(time.RFC3339))
	default:
		msg += ", no data yet"
	}
	return &backend.CheckHealthResult{Status: backend.HealthStatusOk, Message: msg}, nil
}

// CallResource backs the query builder: sets, fields, labels, values, and
// the parse/print pair that keeps text and builder in step. There is one
// parser, in Go, and the frontend never implements a second one.
func (d *Datasource) CallResource(ctx context.Context, req *backend.CallResourceRequest, sender backend.CallResourceResponseSender) error {
	send := func(code int, v any) error {
		body, err := json.Marshal(v)
		if err != nil {
			return err
		}
		return sender.Send(&backend.CallResourceResponse{
			Status:  code,
			Headers: map[string][]string{"Content-Type": {"application/json"}},
			Body:    body,
		})
	}
	// Fetched only where it is read. In proxy mode Catalogue is a network
	// round trip to the store, and "parse" is the path the query editor
	// calls on every keystroke -- neither it nor "print" looks at the
	// result, so fetching it there bought a request per character typed.
	var cat wire.Catalogue
	switch req.Path {
	case "sets", "fields", "labels":
		var err error
		if cat, err = d.svc.Catalogue(ctx); err != nil {
			return send(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
	}

	switch req.Path {
	case "sets":
		names := make([]string, 0, len(cat.Sets))
		for _, s := range cat.Sets {
			names = append(names, s.Name)
		}
		return send(http.StatusOK, map[string]any{"sets": names})
	case "fields":
		set := queryParam(req.URL, "set")
		for _, s := range cat.Sets {
			if s.Name == set {
				return send(http.StatusOK, s)
			}
		}
		return send(http.StatusNotFound, map[string]string{"error": "unknown set " + set})
	case "labels":
		set := queryParam(req.URL, "set")
		for _, s := range cat.Sets {
			if s.Name == set {
				return send(http.StatusOK, map[string]any{"labels": s.Labels})
			}
		}
		return send(http.StatusNotFound, map[string]string{"error": "unknown set " + set})
	case "label-values":
		key := queryParam(req.URL, "key")
		vals, err := d.svc.LabelValues(ctx, key)
		if err != nil {
			return send(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
		return send(http.StatusOK, map[string]any{"key": key, "values": vals})
	case "parse":
		var body struct {
			Text string `json:"text"`
		}
		if err := json.Unmarshal(req.Body, &body); err != nil {
			return send(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}
		ast, warns, err := d.svc.Parse(ctx, body.Text)
		if err != nil {
			return send(http.StatusOK, map[string]any{"error": err.Error()})
		}
		return send(http.StatusOK, map[string]any{"ast": ast, "warnings": warns})
	case "print":
		var q mql.Query
		if err := json.Unmarshal(req.Body, &q); err != nil {
			return send(http.StatusBadRequest, map[string]string{"error": err.Error()})
		}
		return send(http.StatusOK, map[string]any{"text": mql.Print(&q)})
	}
	return send(http.StatusNotFound, map[string]string{"error": "unknown resource " + req.Path})
}

// queryParam reads one query parameter, percent-decoded. A hand-rolled
// scan that skipped decoding would hand "my%20set" to the catalogue and
// report it as an unknown set.
func queryParam(rawURL, key string) string {
	i := strings.IndexByte(rawURL, '?')
	if i < 0 {
		return ""
	}
	vals, err := url.ParseQuery(rawURL[i+1:])
	if err != nil {
		return ""
	}
	return vals.Get(key)
}
