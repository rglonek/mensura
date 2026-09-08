package store

import (
	"context"
	"encoding/binary"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/rglonek/mensura/internal/engine"
	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/mql"
	"github.com/rglonek/mensura/pkg/render"
	"github.com/rglonek/mensura/pkg/wire"
)

// Query executes an MQL AST. A tripped safety gate returns results plus an
// error string, never a bare error: an operator narrowing filters wants the
// partial shape and the reason.
func (s *Store) Query(ctx context.Context, req *wire.QueryRequest) (*wire.QueryResponse, error) {
	started := time.Now()
	if req.AST == nil {
		return nil, fmt.Errorf("query: no AST supplied")
	}
	q := req.AST

	// The datasource ceilings, with a disabled gate expressed as 0. The
	// validator and the executor must agree on these, or a query that
	// switched a gate off is still rejected for exceeding it.
	seriesCeiling := s.cfg.MaxSeriesPerGraph
	if req.Options.DisableSeriesSafety {
		seriesCeiling = 0
	}
	pointsCeiling := s.cfg.MaxDataPointsReceived
	if req.Options.DisableSizeSafety {
		pointsCeiling = 0
	}

	// Validation runs before the auxiliary forms, not after them. The
	// switch below used to return first, so a LABELS query's WHERE was
	// never shape-checked: a predicate node carrying two arms of the
	// tagged union executed as whichever arm the lowering reaches first
	// and silently dropped the rest, which is how `LABELS host WHERE dc =
	// "eu-west-1"` could return every datacentre's hosts again.
	warns, verr := mql.Validate(q, s.Schema(), seriesCeiling, pointsCeiling)
	if verr != nil {
		return nil, verr
	}

	switch q.Kind {
	case mql.KindSets:
		return s.querySets(), nil
	case mql.KindFields:
		return s.queryFields(q.From)
	case mql.KindLabelKeys:
		return s.queryLabelKeys(q.From), nil
	case mql.KindLabels:
		resp, err := s.queryLabelValues(ctx, q, req, narrower(seriesCeiling, q.Limits.Series), narrower(pointsCeiling, q.Limits.Points))
		if err == nil && resp != nil {
			resp.Warnings = append(warns, resp.Warnings...)
		}
		return resp, err
	}

	// LIMIT may only narrow, never widen — Validate has already refused a
	// limit above the ceiling — so it is applied on top.
	maxSeries := narrower(seriesCeiling, q.Limits.Series)
	maxPointsIn := narrower(pointsCeiling, q.Limits.Points)
	if req.Options.FromAlert && q.EveryMs == nil {
		warns = append(warns, mql.Diag{Code: "W302", Msg: "alert rules should set EVERY; a pixel-derived window is not a stable basis for a rule"})
	}

	// One execution slot per query. A scan buffers every point it groups,
	// so letting an unbounded number of them run is how a busy dashboard
	// turns into an out-of-memory kill.
	select {
	case s.jobs <- struct{}{}:
		defer func() { <-s.jobs }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	plan, planWarns, err := s.plan(q, req)
	if err != nil {
		return nil, err
	}
	warns = append(warns, planWarns...)

	// Series is always a list, never null: a client should not have to
	// special-case "no results" differently from "no series".
	resp := &wire.QueryResponse{Warnings: warns, Series: []wire.Series{}}
	if plan.impossible {
		resp.Stats.DurationMs = time.Since(started).Milliseconds()
		return resp, nil
	}

	switch q.Format {
	case mql.FormatTable, mql.FormatLogs:
		err = s.runTabular(ctx, q, req, plan, resp)
	case mql.FormatHeatmap:
		err = s.runHeatmap(ctx, q, req, plan, resp, maxSeries, maxPointsIn)
	default:
		err = s.runTimeseries(ctx, q, req, plan, resp, maxSeries, maxPointsIn)
	}
	if err != nil {
		return nil, err
	}
	resp.Stats.DurationMs = time.Since(started).Milliseconds()
	return resp, nil
}

// narrower applies a per-query LIMIT on top of a datasource ceiling. Zero
// means "no gate", so a limit always wins over an absent ceiling.
func narrower(ceiling int, limit *int) int {
	if limit == nil || *limit <= 0 {
		return ceiling
	}
	if ceiling == 0 || *limit < ceiling {
		return *limit
	}
	return ceiling
}

// queryPlan is what the planner produced: which shards to scan, what to
// push down, and which columns to decode.
type queryPlan struct {
	shards     []string
	expr       engine.Expr
	projection []string
	fields     []resolvedField
	impossible bool
	// reverse walks the range newest-first, which is what a logs view
	// wants: reading forward to a LIMIT returns the oldest matches in the
	// range and never the most recent ones.
	reverse bool
}

// resolvedField is one selected field with its modifiers resolved against
// the catalogue defaults.
type resolvedField struct {
	name  string // column to read
	label string // display name
	spec  render.Spec
	meta  *wire.FieldMeta
}

func (s *Store) plan(q *mql.Query, req *wire.QueryRequest) (*queryPlan, []mql.Diag, error) {
	p := &queryPlan{}
	var warns []mql.Diag

	p.shards = s.shardsFor(q.From, req.FromMs, req.ToMs)
	p.reverse = q.Format == mql.FormatLogs

	proj := map[string]struct{}{model.TimestampField: {}}
	for _, l := range q.By {
		proj[l] = struct{}{}
	}
	for _, fe := range q.Select {
		if fe.Histogram != "" {
			s.mu.RLock()
			e, ok := s.catalogue[q.From]
			var buckets []string
			if ok {
				buckets = e.BucketSets[fe.Histogram].Buckets
			}
			s.mu.RUnlock()
			for _, b := range buckets {
				if b != "" {
					proj[b] = struct{}{}
				}
			}
			continue
		}
		proj[fe.Field] = struct{}{}
		p.fields = append(p.fields, s.resolveField(q.From, fe))
	}

	expr, exprWarns, impossible := s.buildExpr(q.From, q.Where, proj)
	warns = append(warns, exprWarns...)
	p.impossible = impossible
	p.expr = expr

	// Required fields become existence predicates so the scan skips rows
	// that could not contribute.
	var requires []engine.Expr
	for _, fe := range q.Select {
		if fe.Modifiers.Required && fe.Field != "" {
			requires = append(requires, engine.Exists(fe.Field))
		}
	}
	if len(requires) > 0 {
		if p.expr == nil {
			p.expr = engine.And(requires...)
		} else {
			p.expr = engine.And(append([]engine.Expr{p.expr}, requires...)...)
		}
	}

	for c := range proj {
		p.projection = append(p.projection, c)
	}
	sort.Strings(p.projection)
	return p, warns, nil
}

// resolveField merges query modifiers with catalogue metadata. Metadata
// supplies defaults only: an explicit modifier always wins, and the
// resolved result is what Explain reports, so nothing is invisible.
func (s *Store) resolveField(set string, fe mql.FieldExpr) resolvedField {
	rf := resolvedField{name: fe.Field, label: fe.Name()}
	m := fe.Modifiers
	rf.spec = render.Spec{
		Delta:     m.Delta,
		PerSecond: m.PerSecond,
		Negate:    m.Negate,
		SSE:       render.SSE{Mode: render.SSEConst},
	}
	if m.SSE != nil {
		switch m.SSE.Mode {
		case "repeat":
			rf.spec.SSE = render.SSE{Mode: render.SSERepeat}
		case "off":
			rf.spec.SSE = render.SSE{Mode: render.SSEOff}
		default:
			rf.spec.SSE = render.SSE{Mode: render.SSEConst, Value: m.SSE.Value}
		}
	}
	info, known := s.Schema().Field(set, fe.Field)
	switch {
	case m.GapMs != nil:
		rf.spec.GapMs = *m.GapMs
	case known:
		rf.spec.GapMs = info.MaxInterval
	}
	switch {
	case m.Clamp != nil:
		rf.spec.ClampMin, rf.spec.ClampMax = m.Clamp.Min, m.Clamp.Max
		rf.spec.ClampElseRaw = m.Clamp.Else == "raw"
	case known && (info.LimitMin != nil || info.LimitMax != nil):
		// A field that declared limits gets the counter-reset escape hatch
		// wired up by default.
		rf.spec.ClampMin, rf.spec.ClampMax = info.LimitMin, info.LimitMax
		rf.spec.ClampElseRaw = true
	}
	if known {
		rf.meta = &wire.FieldMeta{
			Set: set, Field: fe.Field, Kind: info.Kind, Unit: info.Unit,
			UnitHint: info.UnitHint, Description: info.Description,
		}
	}
	return rf
}

// buildExpr lowers an MQL predicate onto engine expressions, resolving
// label values through the dictionary so every comparison becomes integer
// equality at scan time.
func (s *Store) buildExpr(set string, e mql.Expr, proj map[string]struct{}) (engine.Expr, []mql.Diag, bool) {
	if e.Empty() {
		return nil, nil, false
	}
	var warns []mql.Diag
	var impossible bool

	var lower func(e mql.Expr) engine.Expr
	lower = func(e mql.Expr) engine.Expr {
		switch {
		case len(e.And) > 0:
			subs := make([]engine.Expr, 0, len(e.And))
			for _, sub := range e.And {
				subs = append(subs, lower(sub))
			}
			return engine.And(subs...)
		case len(e.Or) > 0:
			subs := make([]engine.Expr, 0, len(e.Or))
			for _, sub := range e.Or {
				subs = append(subs, lower(sub))
			}
			return engine.Or(subs...)
		case e.Not != nil:
			return engine.Not(lower(*e.Not))
		case e.Has != "":
			proj[e.Has] = struct{}{}
			return engine.Exists(e.Has)
		case e.Missing != "":
			proj[e.Missing] = struct{}{}
			return engine.Not(engine.Exists(e.Missing))
		case e.Eq != nil:
			proj[e.Eq.Label] = struct{}{}
			idx, ok := s.lookup(e.Eq.Label, e.Eq.Value)
			if !ok {
				warns = append(warns, mql.Diag{Code: "W201", Msg: fmt.Sprintf("no values match %s = %q", e.Eq.Label, e.Eq.Value)})
				return engine.Const(false)
			}
			return engine.Eq(e.Eq.Label, model.Int(int64(idx)))
		case e.Ne != nil:
			proj[e.Ne.Label] = struct{}{}
			idx, ok := s.lookup(e.Ne.Label, e.Ne.Value)
			if !ok {
				// Nothing carries that value, so the inequality holds for
				// every row that carries the label at all.
				return engine.Exists(e.Ne.Label)
			}
			return engine.And(engine.Exists(e.Ne.Label), engine.Not(engine.Eq(e.Ne.Label, model.Int(int64(idx)))))
		case e.In != nil:
			proj[e.In.Label] = struct{}{}
			var vals []model.Value
			var missing []string
			for _, v := range e.In.Values {
				if idx, ok := s.lookup(e.In.Label, v); ok {
					vals = append(vals, model.Int(int64(idx)))
				} else {
					missing = append(missing, v)
				}
			}
			if len(missing) > 0 {
				warns = append(warns, mql.Diag{Code: "W201", Msg: fmt.Sprintf("no values match %s in (%s)", e.In.Label, strings.Join(missing, ", "))})
			}
			if len(vals) == 0 {
				return engine.Const(false)
			}
			return engine.In(e.In.Label, vals...)
		case e.Match != nil, e.NoMatch != nil:
			m := e.Match
			negate := false
			if m == nil {
				m, negate = e.NoMatch, true
			}
			proj[m.Label] = struct{}{}
			re, err := regexp.Compile(m.Regex)
			if err != nil {
				warns = append(warns, mql.Diag{Code: "E001", Msg: fmt.Sprintf("invalid regex /%s/: %v", m.Regex, err)})
				return engine.Const(false)
			}
			// The regex is evaluated once against the dictionary, not once
			// per row: the scan only ever sees an integer set.
			vals, all := s.labelIndicesMatching(m.Label, re.MatchString)
			if len(vals) == all && all > 0 && !negate {
				warns = append(warns, mql.Diag{Code: "W202", Msg: fmt.Sprintf("regex /%s/ matches every value of %s; the clause was folded away", m.Regex, m.Label)})
				return engine.Exists(m.Label)
			}
			if len(vals) == 0 {
				if negate {
					return engine.Exists(m.Label)
				}
				warns = append(warns, mql.Diag{Code: "W201", Msg: fmt.Sprintf("regex /%s/ matches no value of %s", m.Regex, m.Label)})
				return engine.Const(false)
			}
			in := engine.In(m.Label, vals...)
			if negate {
				return engine.And(engine.Exists(m.Label), engine.Not(in))
			}
			return in
		}
		return engine.Const(true)
	}

	expr := lower(e)
	// A predicate that no dictionary value can satisfy means the query
	// cannot match anything; say so up front instead of scanning.
	if isAlwaysFalse(e, s) {
		impossible = true
	}
	return expr, warns, impossible
}

// isAlwaysFalse reports whether the top level of a predicate is a single
// comparison that no dictionary value can satisfy.
func isAlwaysFalse(e mql.Expr, s *Store) bool {
	switch {
	case e.Eq != nil:
		_, ok := s.lookup(e.Eq.Label, e.Eq.Value)
		return !ok
	case e.In != nil:
		for _, v := range e.In.Values {
			if _, ok := s.lookup(e.In.Label, v); ok {
				return false
			}
		}
		return true
	case len(e.And) > 0:
		for _, sub := range e.And {
			if isAlwaysFalse(sub, s) {
				return true
			}
		}
	}
	return false
}

// ---------- execution ----------

type seriesAcc struct {
	name   string
	labels map[string]string
	field  resolvedField
	points []render.Point
}

func (s *Store) runTimeseries(ctx context.Context, q *mql.Query, req *wire.QueryRequest, p *queryPlan, resp *wire.QueryResponse, maxSeries, maxPoints int) error {
	acc := map[string]*seriesAcc{}
	var gateErr string
	points := 0

	err := s.scan(ctx, p, req, &resp.Stats, func(row engine.Row) bool {
		ts, ok := row[model.TimestampField].AsInt()
		if !ok {
			return true
		}
		labels := s.rowLabels(row, q.By)
		for i := range p.fields {
			f := p.fields[i]
			v, ok := row[f.name]
			if !ok {
				continue
			}
			fv, ok := v.AsFloat()
			if !ok {
				continue
			}
			points++
			if maxPoints > 0 && points > maxPoints {
				gateErr = "too many datapoints received; zoom in or add filters"
				return false
			}
			key := seriesKey(labels, q.By, f.label)
			a, ok := acc[key]
			if !ok {
				if maxSeries > 0 && len(acc) >= maxSeries {
					gateErr = "too many series for one graph; add filters or reduce BY"
					return false
				}
				a = &seriesAcc{name: seriesName(labels, q.By, f.label), labels: labels, field: f}
				acc[key] = a
			}
			a.points = append(a.points, render.Point{Value: fv, TSMs: ts})
		}
		return true
	})
	if err != nil {
		return err
	}

	window := render.Window(req.ToMs-req.FromMs, req.MaxPoints, req.IntervalMs)
	if q.EveryMs != nil {
		window = *q.EveryMs
	}
	if window < 0 {
		// Validate refuses a non-positive EVERY, but this value is also
		// reached by a caller that builds a wire.QueryRequest directly.
		// A negative window makes every boundary test fire, so each
		// sample becomes its own window.
		window = 0
	}
	out := make([]*seriesAcc, 0, len(acc))
	for _, a := range acc {
		out = append(out, a)
	}
	// Sorting by name keeps colour assignment stable across reloads.
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })

	for _, a := range out {
		spec := a.field.spec
		// The end of the range, so a series that stops mid-range draws a
		// connect-break where the cadence was first missed instead of
		// running to its last point and stopping.
		spec.EndMs = req.ToMs
		if req.Options.FromAlert {
			// Synthetic padding must never contribute to a rule evaluation.
			spec.SSE = render.SSE{Mode: render.SSEOff}
			spec.EndMs = 0
		}
		pts := render.Series(a.points, spec, window)
		ser := wire.Series{Name: a.name, Labels: a.labels, Meta: a.field.meta}
		for _, o := range pts {
			ser.TSMs = append(ser.TSMs, o.TSMs)
			ser.Values = append(ser.Values, o.Value)
			ser.IsNull = append(ser.IsNull, o.Null)
		}
		resp.Stats.PointsOut += len(pts)
		resp.Series = append(resp.Series, ser)
	}
	resp.Stats.SeriesCount = len(resp.Series)
	if gateErr != "" {
		resp.Error = gateErr
		resp.Stats.Truncated = true
		resp.Warnings = append(resp.Warnings, mql.Diag{Code: "W401", Msg: gateErr})
	}
	return nil
}

// runHeatmap sums bucket counts per window. Extremes are the right summary
// for a line; a heatmap wants totals, so this path aggregates rather than
// running the min/max walk.
func (s *Store) runHeatmap(ctx context.Context, q *mql.Query, req *wire.QueryRequest, p *queryPlan, resp *wire.QueryResponse, maxSeries, maxPoints int) error {
	if len(q.Select) == 0 {
		return fmt.Errorf("query: FORMAT heatmap requires HISTOGRAM(<bucket set>)")
	}
	s.mu.RLock()
	entry, ok := s.catalogue[q.From]
	var bs wire.BucketSetInfo
	name := q.Select[0].Histogram
	if ok {
		// Copied, not aliased: the buckets and edges travel into the
		// response and are marshalled long after the lock is released,
		// while a concurrent write carrying bucket metadata rewrites the
		// same slices in place.
		stored := entry.BucketSets[name]
		bs = wire.BucketSetInfo{
			Buckets: append([]string(nil), stored.Buckets...),
			Edges:   append([]float64(nil), stored.Edges...),
			Unit:    stored.Unit,
		}
	}
	s.mu.RUnlock()
	if len(bs.Buckets) == 0 {
		return fmt.Errorf("query: bucket set %q has no buckets on set %q", name, q.From)
	}

	window := render.Window(req.ToMs-req.FromMs, req.MaxPoints, req.IntervalMs)
	if q.EveryMs != nil {
		window = *q.EveryMs
	}
	if window <= 0 {
		window = 1
	}
	type key struct {
		group  string
		bucket int
	}
	sums := map[key]map[int64]float64{}
	groups := map[string]map[string]string{}
	// The heatmap path used to accumulate without any bound, so the two
	// datasource ceilings that stop a busy dashboard from turning into an
	// out-of-memory kill applied to timeseries queries and not to this
	// one. A group x bucket pair is a series; a distinct window inside one
	// is a point.
	var gateErr string
	points := 0

	err := s.scan(ctx, p, req, &resp.Stats, func(row engine.Row) bool {
		ts, ok := row[model.TimestampField].AsInt()
		if !ok {
			return true
		}
		labels := s.rowLabels(row, q.By)
		g := seriesKey(labels, q.By, "")
		groups[g] = labels
		bucketTs := floorTo(ts, window)
		for bi, col := range bs.Buckets {
			if col == "" {
				continue
			}
			v, ok := row[col]
			if !ok {
				continue
			}
			fv, ok := v.AsFloat()
			if !ok {
				continue
			}
			k := key{g, bi}
			cell, live := sums[k]
			if !live {
				if maxSeries > 0 && len(sums) >= maxSeries {
					gateErr = "too many series for one heatmap; add filters or reduce BY"
					return false
				}
				cell = map[int64]float64{}
				sums[k] = cell
			}
			if _, seen := cell[bucketTs]; !seen {
				points++
				if maxPoints > 0 && points > maxPoints {
					gateErr = "too many datapoints received; zoom in or add filters"
					return false
				}
			}
			cell[bucketTs] += fv
		}
		return true
	})
	if err != nil {
		return err
	}

	var keys []key
	for k := range sums {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].group != keys[j].group {
			return keys[i].group < keys[j].group
		}
		return keys[i].bucket < keys[j].bucket
	})
	for _, k := range keys {
		buckets := sums[k]
		var times []int64
		for t := range buckets {
			times = append(times, t)
		}
		sort.Slice(times, func(i, j int) bool { return times[i] < times[j] })
		ser := wire.Series{
			Name:        heatmapSeriesName(groups[k.group], q.By, bs, k.bucket),
			Labels:      groups[k.group],
			BucketEdges: bs.Edges,
		}
		for _, t := range times {
			ser.TSMs = append(ser.TSMs, t)
			ser.Values = append(ser.Values, buckets[t])
			ser.IsNull = append(ser.IsNull, false)
		}
		resp.Stats.PointsOut += len(times)
		resp.Series = append(resp.Series, ser)
	}
	resp.Stats.SeriesCount = len(resp.Series)
	if gateErr != "" {
		resp.Error = gateErr
		resp.Stats.Truncated = true
		resp.Warnings = append(resp.Warnings, mql.Diag{Code: "W401", Msg: gateErr})
	}
	return nil
}

// floorTo rounds a timestamp down onto a window boundary. Go's % keeps the
// sign of the dividend, so `ts - ts%w` rounds a negative timestamp *up*
// and puts it in the following window.
func floorTo(ts, window int64) int64 {
	if window <= 0 {
		return ts
	}
	m := ts % window
	if m < 0 {
		m += window
	}
	return ts - m
}

func heatmapSeriesName(labels map[string]string, by []string, bs wire.BucketSetInfo, bucket int) string {
	edge := ""
	if bucket < len(bs.Edges) {
		edge = fmt.Sprintf("%g", bs.Edges[bucket])
	}
	return seriesName(labels, by, edge)
}

// runTabular serves table and logs formats: raw rows in time order, no
// downsampling and no gap injection, bounded by LIMIT POINTS.
func (s *Store) runTabular(ctx context.Context, q *mql.Query, req *wire.QueryRequest, p *queryPlan, resp *wire.QueryResponse) error {
	limit := 1000
	if q.Limits.Points != nil {
		limit = *q.Limits.Points
	}
	// Validate rejects a non-positive LIMIT, but this is the value that
	// becomes a slice bound below and it is reached by any caller that
	// builds a wire.QueryRequest directly, so it is clamped here too
	// rather than trusted. rows[:-1] panics, and in plugin mode that
	// panic is not recovered: it takes down the process that owns the
	// data directory.
	if limit < 0 {
		limit = 0
	}
	resp.Columns = append(resp.Columns, wire.Column{Name: "time", Type: "time"})
	for _, l := range q.By {
		resp.Columns = append(resp.Columns, wire.Column{Name: l, Type: "string"})
	}
	for _, f := range p.fields {
		typ := "number"
		if f.meta != nil && f.meta.Kind == model.KindString {
			typ = "string"
		}
		resp.Columns = append(resp.Columns, wire.Column{Name: f.label, Type: typ})
	}

	type tsRow struct {
		ts   int64
		vals []any
	}
	var rows []tsRow
	err := s.scan(ctx, p, req, &resp.Stats, func(row engine.Row) bool {
		ts, ok := row[model.TimestampField].AsInt()
		if !ok {
			return true
		}
		labels := s.rowLabels(row, q.By)
		vals := make([]any, 0, len(resp.Columns))
		vals = append(vals, ts)
		for _, l := range q.By {
			vals = append(vals, labels[l])
		}
		carries := false
		for _, f := range p.fields {
			v, ok := row[f.name]
			if !ok {
				vals = append(vals, nil)
				continue
			}
			if v.T == model.TypeString {
				carries = true
				vals = append(vals, v.S)
				continue
			}
			// The verdict is honoured, not discarded. A value AsFloat
			// cannot represent -- a non-finite one -- used to be appended
			// anyway, and a NaN in a table cell makes encoding/json fail
			// on the whole response after the 200 header has gone out.
			// An empty cell is the honest rendering, and it is the one
			// the absent-column branch above already uses.
			fv, ok := v.AsFloat()
			if !ok {
				vals = append(vals, nil)
				continue
			}
			carries = true
			vals = append(vals, fv)
		}
		if !carries {
			return true
		}
		rows = append(rows, tsRow{ts, vals})
		return len(rows) <= limit
	})
	if err != nil {
		return err
	}
	// The scan stops one row past the limit, so an extra row is the signal
	// that the range held more than was asked for.
	truncated := len(rows) > limit
	if truncated {
		rows = rows[:limit]
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].ts < rows[j].ts })
	resp.Stats.Truncated = truncated
	if truncated {
		// Truncation used to be recorded in Stats alone, which nothing
		// renders: a table quietly showed the first rows of a range with
		// nothing saying there were more. The timeseries and heatmap
		// paths say so with the same error and the same code.
		msg := fmt.Sprintf("more rows matched than the limit of %d; the newest were kept -- narrow the range, add filters or raise LIMIT POINTS", limit)
		if !p.reverse {
			msg = fmt.Sprintf("more rows matched than the limit of %d; the oldest were kept -- narrow the range, add filters or raise LIMIT POINTS", limit)
		}
		resp.Error = msg
		resp.Warnings = append(resp.Warnings, mql.Diag{Code: "W401", Msg: msg})
	}
	for _, r := range rows {
		resp.Rows = append(resp.Rows, wire.Row{Values: r.vals})
	}
	return nil
}

// scan walks every shard the plan selected, honouring the request context
// so a client disconnect unwinds iteration rather than finishing it.
//
// stats, when non-nil, accumulates what the scan touched. Those two
// numbers are part of the query response and used to be left at zero on
// every query.
func (s *Store) scan(ctx context.Context, p *queryPlan, req *wire.QueryRequest, stats *wire.QueryStats, visit func(engine.Row) bool) error {
	shards := p.shards
	if p.reverse {
		// Newest shard first, so a bounded newest-first scan can stop
		// without reading the whole range.
		shards = make([]string, len(p.shards))
		for i, name := range p.shards {
			shards[len(p.shards)-1-i] = name
		}
	}
	for _, shard := range shards {
		q := s.db.Query(shard).
			Between(model.TimestampField, req.FromMs, req.ToMs).
			Project(p.projection...).
			Reverse(p.reverse)
		if p.expr != nil {
			q = q.Where(p.expr)
		}
		it, err := q.Run(ctx)
		if err == engine.ErrUnknownSet {
			continue
		}
		if err != nil {
			return err
		}
		if stats != nil {
			stats.ShardsScanned++
		}
		stop := false
		for it.Next() {
			_, row := it.Record()
			if stats != nil {
				stats.RowsScanned++
			}
			if !visit(row) {
				stop = true
				break
			}
		}
		errIt := it.Err()
		_ = it.Close()
		if errIt != nil {
			return errIt
		}
		if stop {
			break
		}
	}
	return nil
}

// rowLabels translates the dictionary indices on a row back into strings.
func (s *Store) rowLabels(row engine.Row, by []string) map[string]string {
	out := make(map[string]string, len(by))
	for _, l := range by {
		v, ok := row[l]
		if !ok {
			continue
		}
		idx, ok := v.AsInt()
		if !ok {
			continue
		}
		if str, ok := s.labelValue(l, int32(idx)); ok {
			out[l] = str
		}
	}
	return out
}

// seriesKey is the grouping identity of one series: the BY values in
// declared order, then the field's display name.
//
// Every component is length-prefixed rather than separated by a delimiter,
// for the reason model.PrimaryKey is: a label value is any valid UTF-8, so
// it may contain the '=' and the NUL that framed it. With delimiters,
// {a: "x", ab: "y\x00ab=z"} and {a: "x\x00ab=y", ab: "z"} built the same
// key, so two distinct series were silently merged into one plotted line.
func seriesKey(labels map[string]string, by []string, field string) string {
	var b strings.Builder
	for _, l := range by {
		writeKeyPart(&b, l)
		writeKeyPart(&b, labels[l])
	}
	writeKeyPart(&b, field)
	return b.String()
}

func writeKeyPart(b *strings.Builder, s string) {
	var buf [binary.MaxVarintLen64]byte
	b.Write(buf[:binary.PutUvarint(buf[:], uint64(len(s)))])
	b.WriteString(s)
}

// seriesName is the legend: the BY values in declared order, then the
// field's display name.
//
// Every BY slot is rendered, including one whose label the row did not
// carry. Dropping those collapsed the legend onto fewer slots than the
// grouping key has, so {host: "a", pool: ""} and {host: "", pool: "a"}
// are two series by seriesKey and drew as two lines both labelled "a" --
// indistinguishable in the panel, and in Grafana's own legend picker. An
// absent slot renders as "<label>=", which names the label that is
// missing and keeps the remaining values in their declared positions.
func seriesName(labels map[string]string, by []string, field string) string {
	parts := make([]string, 0, len(by)+1)
	for _, l := range by {
		if v := labels[l]; v != "" {
			parts = append(parts, v)
			continue
		}
		parts = append(parts, l+"=")
	}
	if field != "" {
		parts = append(parts, field)
	}
	return strings.Join(parts, " : ")
}

// ---------- auxiliary query forms ----------

func (s *Store) querySets() *wire.QueryResponse {
	resp := &wire.QueryResponse{Series: []wire.Series{}, Columns: []wire.Column{{Name: "set", Type: "string"}}}
	for _, n := range s.Sets() {
		resp.Rows = append(resp.Rows, wire.Row{Values: []any{n}})
	}
	return resp
}

func (s *Store) queryFields(set string) (*wire.QueryResponse, error) {
	// Everything the rows are built from is copied out under the lock: a
	// concurrent write widening this set's field map would otherwise be a
	// concurrent map read and write, which is fatal, not merely racy.
	type fieldRow struct {
		name        string
		kind        string
		unit        string
		maxInterval int64
	}
	s.mu.RLock()
	e, ok := s.catalogue[set]
	var rows []fieldRow
	if ok {
		rows = make([]fieldRow, 0, len(e.Fields))
		for n, f := range e.Fields {
			rows = append(rows, fieldRow{n, string(f.Kind), f.Unit, f.MaxInterval})
		}
	}
	s.mu.RUnlock()
	if !ok {
		return nil, mql.Diag{Code: "E002", Msg: fmt.Sprintf("unknown set %q", set)}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].name < rows[j].name })
	resp := &wire.QueryResponse{Series: []wire.Series{}, Columns: []wire.Column{
		{Name: "field", Type: "string"}, {Name: "kind", Type: "string"},
		{Name: "unit", Type: "string"}, {Name: "max_interval_ms", Type: "number"},
	}}
	for _, r := range rows {
		resp.Rows = append(resp.Rows, wire.Row{Values: []any{r.name, r.kind, r.unit, float64(r.maxInterval)}})
	}
	return resp, nil
}

func (s *Store) queryLabelKeys(set string) *wire.QueryResponse {
	resp := &wire.QueryResponse{Series: []wire.Series{}, Columns: []wire.Column{{Name: "label", Type: "string"}}}
	for _, k := range s.LabelKeys(set) {
		resp.Rows = append(resp.Rows, wire.Row{Values: []any{k}})
	}
	return resp
}

// queryLabelValues backs `LABELS <key> [WHERE <predicate>]`.
//
// Without a filter this is a plain dictionary read. With one it is the
// filter scan that 06-query.md section 5 describes: the distinct values
// of the key among rows that match, across every set carrying that label
// and inside the request's time range. The predicate used to be parsed,
// stored in the AST, printed back — and then silently dropped here, so a
// dashboard variable written as `LABELS host WHERE dc = "eu-west-1"`
// returned the hosts of every datacentre.
func (s *Store) queryLabelValues(ctx context.Context, q *mql.Query, req *wire.QueryRequest, maxSeries, maxPoints int) (*wire.QueryResponse, error) {
	resp := &wire.QueryResponse{Series: []wire.Series{}, Columns: []wire.Column{{Name: "value", Type: "string"}}}
	if q.Where.Empty() {
		for _, v := range s.LabelValues(q.Label) {
			resp.Rows = append(resp.Rows, wire.Row{Values: []any{v}})
		}
		return resp, nil
	}

	// A filter scan costs what a query costs, so it takes a job slot.
	select {
	case s.jobs <- struct{}{}:
		defer func() { <-s.jobs }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// The same two ceilings the graph paths are bounded by. This scan
	// walks every set carrying the label across the whole time range and
	// accumulates into a map, and it is the path a dashboard hits on
	// every variable refresh, so leaving it unbounded made a variable
	// query the cheapest way to exhaust the process.
	seen := map[string]struct{}{}
	var gateErr string
	points := 0
	// One pass over the shard list for the whole scan. shardsFor walks and
	// sorts every set name in the store, so calling it once per set made
	// this quadratic in the shard count on the path a dashboard hits on
	// every variable refresh.
	byLogical := s.shardsByLogical()
	for _, set := range s.setsWithLabel(q.Label) {
		if gateErr != "" {
			break
		}
		proj := map[string]struct{}{model.TimestampField: {}, q.Label: {}}
		expr, warns, impossible := s.buildExpr(set, q.Where, proj)
		resp.Warnings = append(resp.Warnings, warns...)
		if impossible {
			continue
		}
		p := &queryPlan{
			shards: shardsInRange(set, byLogical[set], req.FromMs, req.ToMs),
			expr:   expr,
		}
		for c := range proj {
			p.projection = append(p.projection, c)
		}
		sort.Strings(p.projection)
		err := s.scan(ctx, p, req, &resp.Stats, func(row engine.Row) bool {
			v, ok := row[q.Label]
			if !ok {
				return true
			}
			idx, ok := v.AsInt()
			if !ok {
				return true
			}
			points++
			if maxPoints > 0 && points > maxPoints {
				gateErr = "too many datapoints received; narrow the range or add filters to the LABELS predicate"
				return false
			}
			str, ok := s.labelValue(q.Label, int32(idx))
			if !ok {
				return true
			}
			if _, dup := seen[str]; !dup {
				if maxSeries > 0 && len(seen) >= maxSeries {
					gateErr = "too many distinct label values; add filters to the LABELS predicate"
					return false
				}
				seen[str] = struct{}{}
			}
			return true
		})
		if err != nil {
			return nil, err
		}
	}
	out := make([]string, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Strings(out)
	for _, v := range out {
		resp.Rows = append(resp.Rows, wire.Row{Values: []any{v}})
	}
	if gateErr != "" {
		resp.Error = gateErr
		resp.Stats.Truncated = true
		resp.Warnings = append(resp.Warnings, mql.Diag{Code: "W401", Msg: gateErr})
	}
	return resp, nil
}

// setsWithLabel lists the sets whose catalogue entry carries a label key.
func (s *Store) setsWithLabel(key string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []string
	for name, e := range s.catalogue {
		if _, ok := e.Labels[key]; ok {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// Explain reports the plan a query would run, which is what the builder's
// Explain button and the debug endpoint show.
func (s *Store) Explain(q *mql.Query, req *wire.QueryRequest) (map[string]any, error) {
	p, warns, err := s.plan(q, req)
	if err != nil {
		return nil, err
	}
	fields := make([]map[string]any, 0, len(p.fields))
	for _, f := range p.fields {
		fields = append(fields, map[string]any{
			"field": f.name, "as": f.label,
			"delta": f.spec.Delta, "per_second": f.spec.PerSecond, "negate": f.spec.Negate,
			"gap_ms": f.spec.GapMs, "clamp_else_raw": f.spec.ClampElseRaw,
		})
	}
	// Clamped exactly as runTimeseries clamps it. Reporting a window the
	// executor would never use makes Explain describe a plan that is not
	// the plan, which is the one thing this endpoint may not do.
	window := render.Window(req.ToMs-req.FromMs, req.MaxPoints, req.IntervalMs)
	if q.EveryMs != nil {
		window = *q.EveryMs
	}
	if window < 0 {
		window = 0
	}
	return map[string]any{
		"shards":            p.shards,
		"projection":        p.projection,
		"resolved_fields":   fields,
		"downsample_window": window,
		"impossible":        p.impossible,
		"warnings":          warns,
	}, nil
}
