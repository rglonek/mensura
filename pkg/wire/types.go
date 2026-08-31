// Package wire defines the HTTP APIs between components: the write API
// (ingest to store) and the query API (a proxy-mode plugin to store).
// See docs/design/04-wire-protocol.md.
package wire

import (
	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/mql"
)

// ProtocolVersion is the major version carried in the path. Clients refuse
// to start against a store with a different major.
const ProtocolVersion = 1

// WriteRequest is the body of POST /v1/write.
type WriteRequest struct {
	FieldMeta []FieldMeta   `json:"field_meta,omitempty"`
	SetMeta   []SetMeta     `json:"set_meta,omitempty"`
	Batches   []model.Batch `json:"batches"`
}

// SetMeta carries what an extraction spec declares about a destination set
// as a whole, rather than about one of its fields. Like FieldMeta it is
// sent once per process start and on spec reload, not per sample.
type SetMeta struct {
	Set string `json:"set"`
	// RetentionMs is how long the set is kept; 0 means "keep everything".
	// A nil pointer leaves the store's own setting alone.
	RetentionMs *int64 `json:"retention_ms,omitempty"`
	// ShardMs is the time-shard width. A nil pointer leaves it alone.
	ShardMs *int64 `json:"shard_ms,omitempty"`
	// KeyScheme selects content or offset keying for the set.
	KeyScheme model.KeyScheme `json:"key_scheme,omitempty"`
}

// FieldMeta is the metadata that travels from an extraction spec into the
// catalogue and on into the query builder's defaults. It is sent once per
// process start and on spec reload, not per sample.
type FieldMeta struct {
	Set          string     `json:"set"`
	Field        string     `json:"field"`
	Kind         model.Kind `json:"kind,omitempty"`
	Unit         string     `json:"unit,omitempty"`
	UnitHint     string     `json:"unit_hint,omitempty"`
	Description  string     `json:"description,omitempty"`
	MaxIntervalS int        `json:"max_interval_s,omitempty"`
	LimitMin     *float64   `json:"limit_min,omitempty"`
	LimitMax     *float64   `json:"limit_max,omitempty"`
	BucketSet    string     `json:"bucket_set,omitempty"`
	BucketIndex  int        `json:"bucket_index,omitempty"`
	BucketEdge   float64    `json:"bucket_edge,omitempty"`
}

// WriteResponse reports what happened to a batch. Partial rejection is
// normal: the accepted remainder is committed and the rejects are named.
type WriteResponse struct {
	Accepted         int         `json:"accepted"`
	Duplicate        bool        `json:"duplicate"`
	Rejected         []Rejection `json:"rejected,omitempty"`
	CatalogueVersion int64       `json:"catalogue_version"`
}

type Rejection struct {
	Index  int    `json:"index"`
	Reason string `json:"reason"`
}

// QueryRequest is the body of POST /v1/query.
type QueryRequest struct {
	AST        *mql.Query   `json:"ast"`
	FromMs     int64        `json:"from_ms"`
	ToMs       int64        `json:"to_ms"`
	MaxPoints  int          `json:"max_points"`
	IntervalMs int64        `json:"interval_ms"`
	Format     mql.Format   `json:"format,omitempty"`
	Options    QueryOptions `json:"options,omitempty"`
}

// QueryOptions carries the per-query safety overrides that a dashboard
// exposes as variables: during an incident the operator sometimes really
// does want the expensive query.
type QueryOptions struct {
	DisableSeriesSafety bool `json:"disable_series_safety,omitempty"`
	DisableSizeSafety   bool `json:"disable_size_safety,omitempty"`
	FromAlert           bool `json:"from_alert,omitempty"`
}

// QueryResponse carries results, statistics and diagnostics. A query that
// trips a safety gate returns 200 with both series and Error set: an
// operator narrowing filters wants the partial shape and the reason, not an
// empty panel.
type QueryResponse struct {
	Series   []Series   `json:"series"`
	Rows     []Row      `json:"rows,omitempty"` // table and logs formats
	Columns  []Column   `json:"columns,omitempty"`
	Stats    QueryStats `json:"stats"`
	Warnings []mql.Diag `json:"warnings,omitempty"`
	Error    string     `json:"error,omitempty"`
}

// Series is one rendered series. IsNull is a parallel array rather than a
// NaN sentinel, because NaN is a legitimate value in some sources and
// because "no data" must stay a distinct concept up to the renderer.
type Series struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
	TSMs   []int64           `json:"ts_ms"`
	Values []float64         `json:"values"`
	IsNull []bool            `json:"is_null,omitempty"`
	Meta   *FieldMeta        `json:"meta,omitempty"`
	// BucketEdges is set for heatmap series: the numeric lower bound of
	// each bucket, from the bucket-set declaration.
	BucketEdges []float64 `json:"bucket_edges,omitempty"`
}

type Column struct {
	Name string `json:"name"`
	Type string `json:"type"` // time | number | string
}

type Row struct {
	Values []any `json:"values"`
}

type QueryStats struct {
	ShardsScanned int   `json:"shards_scanned"`
	RowsScanned   int64 `json:"rows_scanned"`
	SeriesCount   int   `json:"series"`
	PointsOut     int   `json:"points"`
	DurationMs    int64 `json:"duration_ms"`
	Truncated     bool  `json:"truncated"`
}

// Hello is the response of GET /v1/hello.
type Hello struct {
	Product          string `json:"product"`
	Version          string `json:"version"`
	Protocol         int    `json:"protocol"`
	Mode             string `json:"mode"`
	DataDir          string `json:"data_dir,omitempty"`
	CatalogueVersion int64  `json:"catalogue_version"`
	UptimeSeconds    int64  `json:"uptime_seconds"`
	// The safety ceilings, so a proxy-mode plugin validates a LIMIT
	// against the same numbers the store will enforce.
	MaxSeriesPerGraph     int `json:"max_series_per_graph,omitempty"`
	MaxDataPointsReceived int `json:"max_datapoints_received,omitempty"`
}

// Catalogue is the response of GET /v1/catalogue: everything the query
// builder needs to offer good defaults.
type Catalogue struct {
	Version   int64               `json:"version"`
	Sets      []SetInfo           `json:"sets"`
	Conflicts []CatalogueConflict `json:"conflicts,omitempty"`
}

type SetInfo struct {
	Name       string                   `json:"name"`
	Fields     map[string]mql.FieldInfo `json:"fields"`
	Labels     []string                 `json:"labels"`
	BucketSets map[string]BucketSetInfo `json:"bucket_sets,omitempty"`
	FirstTSMs  int64                    `json:"first_ts_ms,omitempty"`
	LastTSMs   int64                    `json:"last_ts_ms,omitempty"`
	Shards     []string                 `json:"shards,omitempty"`
}

type BucketSetInfo struct {
	Buckets []string  `json:"buckets"`
	Edges   []float64 `json:"edges"`
	Unit    string    `json:"unit,omitempty"`
}

// CatalogueConflict records disagreeing metadata from two ingesters. The
// last writer wins, but the disagreement is surfaced rather than hidden.
type CatalogueConflict struct {
	Set   string `json:"set"`
	Field string `json:"field"`
	Was   string `json:"was"`
	Now   string `json:"now"`
}

// LabelValues is the response of GET /v1/labels.
type LabelValues struct {
	Key    string   `json:"key"`
	Values []string `json:"values"`
}

// APIError is the body of every non-2xx response.
type APIError struct {
	Error string `json:"error"`
	Code  string `json:"code,omitempty"`
}
