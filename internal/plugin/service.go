package plugin

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

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
	// ParseDiags carries the diagnostics that only the text has: the
	// modifier-order lint cannot be recovered from the AST, because
	// modifiers are a set.
	q, lints, err := mql.ParseDiags(text)
	if err != nil {
		return nil, nil, err
	}
	cfg := l.store.Config()
	warns, verr := mql.Validate(q, l.store.Schema(), cfg.MaxSeriesPerGraph, cfg.MaxDataPointsReceived)
	return q, append(lints, warns...), verr
}

// metadataTTL is how long a fetched catalogue or hello is reused.
//
// It exists because validation is not only a per-panel cost: the query
// editor calls the "parse" resource on every keystroke, and proxy-mode
// validation needs the upstream catalogue and the upstream ceilings. So
// each character typed cost two HTTP round trips to the store, and
// rendering the catalogue on the far side walks every shard -- which is
// exactly the cost CallResource stopped paying by not fetching the
// catalogue for "parse" itself, one layer further in.
//
// A second is long enough to collapse a burst of typing into one fetch
// and short enough that a set or field appearing upstream shows up while
// the author is still looking at the editor. Nothing correctness-critical
// rides on it: the store re-validates every query it executes against its
// own live catalogue.
const metadataTTL = time.Second

// remoteService forwards to a store that owns the data directory. Same
// code path, one network hop.
type remoteService struct {
	client *wire.Client

	mu      sync.Mutex
	cat     wire.Catalogue
	catAt   time.Time
	catOK   bool
	hello   wire.Hello
	helloAt time.Time
	helloOK bool
}

func (r *remoteService) Query(ctx context.Context, req *wire.QueryRequest) (*wire.QueryResponse, error) {
	return r.client.Query(ctx, req)
}

func (r *remoteService) Catalogue(ctx context.Context) (wire.Catalogue, error) {
	r.mu.Lock()
	if r.catOK && time.Since(r.catAt) < metadataTTL {
		cat := r.cat
		r.mu.Unlock()
		return cat, nil
	}
	r.mu.Unlock()
	var cat wire.Catalogue
	if err := r.client.GetJSON(ctx, "/v1/catalogue", &cat); err != nil {
		return wire.Catalogue{}, err
	}
	r.mu.Lock()
	r.cat, r.catAt, r.catOK = cat, time.Now(), true
	r.mu.Unlock()
	return cat, nil
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
	r.mu.Lock()
	if r.helloOK && time.Since(r.helloAt) < metadataTTL {
		h := r.hello
		r.mu.Unlock()
		return h, nil
	}
	r.mu.Unlock()
	var h wire.Hello
	if err := r.client.GetJSON(ctx, "/v1/hello", &h); err != nil {
		return wire.Hello{}, err
	}
	r.mu.Lock()
	r.hello, r.helloAt, r.helloOK = h, time.Now(), true
	r.mu.Unlock()
	return h, nil
}

// Parse is done locally even in proxy mode: the parser is a pure function
// and a round trip per keystroke in the editor would be wasteful. The
// catalogue-dependent validation still comes from the store.
func (r *remoteService) Parse(ctx context.Context, text string) (*mql.Query, []mql.Diag, error) {
	q, lints, err := mql.ParseDiags(text)
	if err != nil {
		return nil, nil, err
	}
	cat, err := r.Catalogue(ctx)
	if err != nil {
		// Not "q, nil, nil": the catalogue-dependent half of validation
		// never ran, and answering with no diagnostics reports a query as
		// valid when nothing checked it. An unreachable store is the
		// answer here.
		return q, lints, fmt.Errorf("the query could not be validated: the store's catalogue is unreachable: %w", err)
	}
	// The upstream store's ceilings, so proxy mode validates a LIMIT the
	// same way embedded mode does rather than accepting anything.
	maxSeries, maxPoints := 0, 0
	if h, herr := r.Hello(ctx); herr == nil {
		maxSeries, maxPoints = h.MaxSeriesPerGraph, h.MaxDataPointsReceived
	}
	warns, verr := mql.Validate(q, catalogueSchema{cat}, maxSeries, maxPoints)
	return q, append(lints, warns...), verr
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
