package plugin

import (
	"context"
	"fmt"
	"net/url"

	"github.com/rglonek/mensura/internal/store"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/wire"
)

// localService is the embedded implementation: the query engine is a
// function call away and the LSM iterator runs in this process.
type localService struct{ store *store.Store }

func (l *localService) Query(ctx context.Context, req *wire.QueryRequest) (*wire.QueryResponse, error) {
	return l.store.Query(ctx, req)
}

func (l *localService) Catalogue(context.Context) (wire.Catalogue, error) {
	return l.store.Catalogue(), nil
}

func (l *localService) LabelValues(_ context.Context, key string) ([]string, error) {
	return l.store.LabelValues(key), nil
}

func (l *localService) Hello(context.Context) (wire.Hello, error) {
	cfg := l.store.Config()
	return wire.Hello{
		Product: "mensura-store", Version: store.Version, Protocol: wire.ProtocolVersion,
		Mode: "plugin", DataDir: cfg.DataDir,
		CatalogueVersion:      l.store.CatalogueVersion(),
		UptimeSeconds:         int64(l.store.Uptime().Seconds()),
		MaxSeriesPerGraph:     cfg.MaxSeriesPerGraph,
		MaxDataPointsReceived: cfg.MaxDataPointsReceived,
	}, nil
}

func (l *localService) Parse(_ context.Context, text string) (*mql.Query, []mql.Diag, error) {
	q, err := mql.Parse(text)
	if err != nil {
		return nil, nil, err
	}
	cfg := l.store.Config()
	warns, verr := mql.Validate(q, l.store.Schema(), cfg.MaxSeriesPerGraph, cfg.MaxDataPointsReceived)
	return q, warns, verr
}

// remoteService forwards to a store that owns the data directory. Same
// code path, one network hop.
type remoteService struct{ client *wire.Client }

func (r *remoteService) Query(ctx context.Context, req *wire.QueryRequest) (*wire.QueryResponse, error) {
	return r.client.Query(ctx, req)
}

func (r *remoteService) Catalogue(ctx context.Context) (wire.Catalogue, error) {
	var cat wire.Catalogue
	err := r.client.GetJSON(ctx, "/v1/catalogue", &cat)
	return cat, err
}

func (r *remoteService) LabelValues(ctx context.Context, key string) ([]string, error) {
	var out wire.LabelValues
	// The key is escaped: an unescaped '&' or '#' would truncate the
	// query string or silently add a parameter.
	path := "/v1/labels?" + url.Values{"key": {key}}.Encode()
	if err := r.client.GetJSON(ctx, path, &out); err != nil {
		return nil, err
	}
	return out.Values, nil
}

func (r *remoteService) Hello(ctx context.Context) (wire.Hello, error) {
	var h wire.Hello
	err := r.client.GetJSON(ctx, "/v1/hello", &h)
	return h, err
}

// Parse is done locally even in proxy mode: the parser is a pure function
// and a round trip per keystroke in the editor would be wasteful. The
// catalogue-dependent validation still comes from the store.
func (r *remoteService) Parse(ctx context.Context, text string) (*mql.Query, []mql.Diag, error) {
	q, err := mql.Parse(text)
	if err != nil {
		return nil, nil, err
	}
	cat, err := r.Catalogue(ctx)
	if err != nil {
		// Not "q, nil, nil": the catalogue-dependent half of validation
		// never ran, and answering with no diagnostics reports a query as
		// valid when nothing checked it. An unreachable store is the
		// answer here.
		return q, nil, fmt.Errorf("the query could not be validated: the store's catalogue is unreachable: %w", err)
	}
	// The upstream store's ceilings, so proxy mode validates a LIMIT the
	// same way embedded mode does rather than accepting anything.
	maxSeries, maxPoints := 0, 0
	if h, herr := r.Hello(ctx); herr == nil {
		maxSeries, maxPoints = h.MaxSeriesPerGraph, h.MaxDataPointsReceived
	}
	warns, verr := mql.Validate(q, catalogueSchema{cat}, maxSeries, maxPoints)
	return q, warns, verr
}

// catalogueSchema adapts a fetched catalogue to the MQL validator.
type catalogueSchema struct{ cat wire.Catalogue }

func (c catalogueSchema) HasSet(set string) bool {
	for _, s := range c.cat.Sets {
		if s.Name == set {
			return true
		}
	}
	return false
}

func (c catalogueSchema) Sets() []string {
	out := make([]string, 0, len(c.cat.Sets))
	for _, s := range c.cat.Sets {
		out = append(out, s.Name)
	}
	return out
}

func (c catalogueSchema) Field(set, field string) (mql.FieldInfo, bool) {
	for _, s := range c.cat.Sets {
		if s.Name != set {
			continue
		}
		f, ok := s.Fields[field]
		return f, ok
	}
	return mql.FieldInfo{}, false
}

func (c catalogueSchema) HasLabel(set, key string) bool {
	for _, s := range c.cat.Sets {
		if s.Name != set {
			continue
		}
		for _, l := range s.Labels {
			if l == key {
				return true
			}
		}
	}
	return false
}

func (c catalogueSchema) BucketSet(set, name string) ([]string, bool) {
	for _, s := range c.cat.Sets {
		if s.Name != set {
			continue
		}
		bs, ok := s.BucketSets[name]
		return bs.Buckets, ok
	}
	return nil, false
}
