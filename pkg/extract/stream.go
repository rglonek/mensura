package extract

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/rglonek/mensura/pkg/model"
)

// Result is one extracted sample, before it is labelled with the stream's
// own labels by the caller.
type Result struct {
	Set    string
	TSMs   int64
	Labels map[string]string
	Fields map[string]model.Value
	Line   string
}

// Stats counts what a stream did, which is what makes a spec debuggable.
type Stats struct {
	Records       int64
	Samples       int64
	Unmatched     int64
	TSParseErrors int64
	Oversize      int64
	// Unjoined counts continuation lines that matched a multiline
	// continue_regex but no join rule, so they were absorbed into
	// nothing.
	Unjoined       int64
	FirstUnmatched []string
}

// Stream is the per-source extraction state: one log file, one connection,
// one followed path. It is not safe for concurrent use; give each source
// its own.
type Stream struct {
	spec    *Spec
	profile *Profile

	loc        *time.Location
	assumeYear int
	tsIdx      int
	lastTS     time.Time
	// lastWall is when lastTS was observed, so a quiet stream can tell
	// how far record time must have advanced since.
	lastWall time.Time

	from, to time.Time

	multiline map[string]*mlBuffer
	aggs      map[string]*aggregator

	Stats Stats

	maxUnmatchedSamples int
}

type mlBuffer struct {
	line string
	ts   time.Time
	seen time.Time
}

type aggregator struct {
	value      float64
	start, end time.Time
	labels     map[string]string
	fields     map[string]model.Value
	set        string
	line       string
	field      string
	mode       string
}

// StreamOptions configure a new stream.
type StreamOptions struct {
	// RefTime resolves year-less timestamp layouts; typically the source
	// file's modification time.
	RefTime time.Time
	// From and To drop records outside a window during extraction, before
	// anything is batched.
	From, To time.Time
}

// NewStream binds a profile to one source.
func (s *Spec) NewStream(p *Profile, opts StreamOptions) (*Stream, error) {
	if p == nil {
		return nil, fmt.Errorf("extract: no profile selected for this stream")
	}
	loc := time.UTC
	if tz := s.Defaults.Timestamp.Timezone; tz != "" && tz != "UTC" {
		l, err := time.LoadLocation(tz)
		if err != nil {
			return nil, fmt.Errorf("extract: timezone %q: %w", tz, err)
		}
		loc = l
	}
	year := time.Now().UTC().Year()
	switch ay := s.Defaults.Timestamp.AssumeYear; {
	case ay == "" || ay == "file-mtime":
		if !opts.RefTime.IsZero() {
			year = opts.RefTime.Year()
		}
	case ay == "now":
	default:
		n, err := strconv.Atoi(ay)
		if err != nil {
			return nil, fmt.Errorf("extract: assume_year %q: expected file-mtime, now or a year", ay)
		}
		year = n
	}
	return &Stream{
		spec: s, profile: p, loc: loc, assumeYear: year, tsIdx: -1,
		from: opts.From, to: opts.To,
		multiline:           map[string]*mlBuffer{},
		aggs:                map[string]*aggregator{},
		maxUnmatchedSamples: 5,
	}, nil
}

// Profile exposes the bound profile, so callers can read field metadata.
func (st *Stream) Profile() *Profile { return st.profile }

// Process handles one record. It returns zero or more samples: zero is
// normal (the record opened a multiline, or fed an aggregator), and an
// error reports why a record produced nothing so the counters stay honest.
func (st *Stream) Process(line string) ([]Result, error) {
	st.Stats.Records++
	if n := st.profile.Framing.MaxRecordBytes; n > 0 && len(line) > n {
		line = trimToRune(line[:n])
		st.Stats.Oversize++
	}
	ts, off, err := st.scanTimestamp(line)
	if err != nil {
		st.Stats.TSParseErrors++
		return nil, err
	}
	st.lastTS, st.lastWall = ts, time.Now()
	if !st.from.IsZero() && ts.Before(st.from) {
		return nil, nil
	}
	if !st.to.IsZero() && ts.After(st.to) {
		return nil, nil
	}
	if off > 0 {
		line = line[off:]
	}

	// Multiline handling: a start marker opens (or replaces) a buffered
	// record; continuation lines append their nominated capture.
	for _, m := range st.profile.Framing.Multiline {
		if strings.Contains(line, m.StartContains) {
			var out []Result
			if buf, ok := st.multiline[m.StartContains]; ok {
				out, _ = st.process(buf.line, buf.ts)
			}
			st.multiline[m.StartContains] = &mlBuffer{line: line, ts: ts, seen: time.Now()}
			return out, nil
		}
		buf, ok := st.multiline[m.StartContains]
		if !ok || m.continueRe == nil || !m.continueRe.MatchString(line) {
			continue
		}
		if ts.Before(buf.ts) {
			// The buffered record is emitted, not thrown away. Deleting
			// it lost a whole record every time a continuation line
			// carried an earlier timestamp -- which interleaved writers
			// produce routinely -- and reported the loss only as a
			// counter.
			delete(st.multiline, m.StartContains)
			out, _ := st.process(buf.line, buf.ts)
			return out, fmt.Errorf("extract: multiline record timestamps moved backwards")
		}
		for i := range m.Join {
			j := &m.Join[i]
			if g := j.re.FindStringSubmatch(line); len(g) > j.Capture {
				buf.line += g[j.Capture]
				buf.seen = time.Now()
				return nil, nil
			}
		}
		// The line is a continuation -- continue_regex claimed it -- but
		// no join rule captured anything from it, so it contributed
		// nothing to the buffered record and nothing of its own. It used
		// to vanish from both the samples and the unmatched tally, which
		// is the one outcome a spec author cannot debug.
		st.Stats.Unjoined++
		return nil, ErrNoJoin
	}
	return st.process(line, ts)
}

// Flush closes every open multiline buffer and aggregation window. Follow
// mode also calls FlushIdle so a quiet stream does not hold a partial
// record or a half-filled window indefinitely.
func (st *Stream) Flush() []Result {
	var out []Result
	for k, buf := range st.multiline {
		delete(st.multiline, k)
		// Partial results are kept even when process reports an error:
		// it returns the windows it closed alongside the failure.
		r, _ := st.process(buf.line, buf.ts)
		out = append(out, r...)
	}
	// Only these are counted here. process() already counted everything
	// it returned, so adding len(out) on top double-counted every
	// flushed multiline record.
	for k, a := range st.aggs {
		delete(st.aggs, k)
		out = append(out, a.emit())
		st.Stats.Samples++
	}
	return out
}

// FlushIdle emits anything that has been waiting longer than the profile's
// idle timeout, measured against wall clock rather than record time.
//
// Aggregation windows are closed here too, not only when a fresh matching
// record arrives: a stream that goes quiet would otherwise hold its last
// half-filled window until shutdown.
func (st *Stream) FlushIdle(now time.Time) []Result {
	var out []Result
	for _, m := range st.profile.Framing.Multiline {
		buf, ok := st.multiline[m.StartContains]
		if !ok || now.Sub(buf.seen) < m.idleTimeout {
			continue
		}
		delete(st.multiline, m.StartContains)
		r, _ := st.process(buf.line, buf.ts)
		out = append(out, r...)
	}
	// closeExpiredAggregators is called directly here, so its results are
	// the only ones this function counts; process() counts its own.
	closed := st.closeExpiredAggregators(st.aggregationHorizon(now))
	out = append(out, closed...)
	st.Stats.Samples += int64(len(closed))
	return out
}

// aggregationHorizon converts wall-clock idleness into the record-time
// clock the aggregators are keyed on. A window may be closed once real
// time has moved past the end of the window that the last record opened,
// which is the only way a quiet stream can make progress.
func (st *Stream) aggregationHorizon(now time.Time) time.Time {
	if st.lastTS.IsZero() {
		return time.Time{}
	}
	// lastWall is when the last record was seen; anything older than that
	// by more than the window width has certainly ended.
	elapsed := now.Sub(st.lastWall)
	if elapsed < 0 {
		elapsed = 0
	}
	return st.lastTS.Add(elapsed)
}

func (st *Stream) process(line string, ts time.Time) ([]Result, error) {
	p := st.profile
	idx := p.matcher.FirstIndex(line)
	if idx < 0 {
		st.Stats.Unmatched++
		if len(st.Stats.FirstUnmatched) < st.maxUnmatchedSamples {
			st.Stats.FirstUnmatched = append(st.Stats.FirstUnmatched, line)
		}
		return nil, ErrNoMatch
	}
	pat := p.Patterns[idx]

	for i := range pat.Replace {
		line = pat.Replace[i].re.ReplaceAllString(line, pat.Replace[i].Sub)
	}

	set := pat.Set
	var groups []string
	var names []string
	for _, re := range pat.extract {
		if g := re.FindStringSubmatch(line); g != nil {
			groups, names = g, re.SubexpNames()
			break
		}
	}
	if groups == nil {
		for i := range pat.Route {
			if g := pat.Route[i].re.FindStringSubmatch(line); g != nil {
				groups, names = g, pat.Route[i].re.SubexpNames()
				set = pat.Route[i].Set
				break
			}
		}
	}
	if groups == nil {
		st.Stats.Unmatched++
		if len(st.Stats.FirstUnmatched) < st.maxUnmatchedSamples {
			st.Stats.FirstUnmatched = append(st.Stats.FirstUnmatched, line)
		}
		return nil, ErrNoMatch
	}

	labels := map[string]string{}
	fields := map[string]model.Value{}
	var histRaw string
	for i, name := range names {
		if i == 0 || name == "" || i >= len(groups) {
			continue
		}
		v := groups[i]
		if name == "buckets" || name == "histogram" {
			histRaw = v
			continue
		}
		if st.isLabel(pat, name) {
			if v != "" {
				labels[name] = v
			}
			continue
		}
		fields[name] = model.Coerce(v)
	}

	if pat.BucketSet != "" {
		if histRaw == "" {
			// Writing a full row of zero counts because no "buckets"
			// group matched would invent data that the source never
			// carried.
			return nil, fmt.Errorf("extract: pattern for set %q declares bucket set %q but captured no buckets group", set, pat.BucketSet)
		}
		bs := p.buckets[pat.BucketSet]
		if err := bs.expand(histRaw, fields); err != nil {
			return nil, err
		}
	}
	for k, v := range pat.DefaultValues {
		if _, ok := fields[k]; !ok {
			fields[k] = model.Coerce(v)
		}
	}
	if len(fields) == 0 && pat.Aggregate == nil {
		// An aggregating pattern legitimately carries no field of its own:
		// the accumulator synthesises one. Any other pattern that extracts
		// only labels would write a row with nothing in it.
		return nil, fmt.Errorf("extract: pattern for set %q produced no fields", set)
	}

	// Aggregation replaces one-row-per-record with one row per window.
	var out []Result
	out = append(out, st.closeExpiredAggregators(ts)...)
	if pat.Aggregate != nil {
		// The error is returned *with* out, never instead of it: the
		// windows in out have already been removed from st.aggs, so a
		// caller that dropped them on the error lost every window that
		// happened to expire on the same record as a spec fault.
		if err := st.aggregate(pat, set, ts, labels, fields, line); err != nil {
			st.Stats.Samples += int64(len(out))
			return out, err
		}
		st.Stats.Samples += int64(len(out))
		return out, nil
	}

	out = append(out, Result{Set: set, TSMs: ts.UnixMilli(), Labels: labels, Fields: fields, Line: line})
	st.Stats.Samples += int64(len(out))
	return out, nil
}

func (st *Stream) isLabel(pat *Pattern, name string) bool {
	if _, ok := st.profile.labelSet[name]; ok {
		return true
	}
	_, ok := pat.labelSet[name]
	return ok
}

func (st *Stream) aggregate(pat *Pattern, set string, ts time.Time, labels map[string]string, fields map[string]model.Value, line string) error {
	ag := pat.Aggregate
	keyParts := make([]string, 0, len(ag.On)+1)
	keyParts = append(keyParts, set)
	for _, on := range ag.On {
		v, ok := labels[on]
		if !ok {
			return fmt.Errorf("extract: aggregation key %q is not a declared label", on)
		}
		keyParts = append(keyParts, on+"="+v)
	}
	key := strings.Join(keyParts, "\x00")

	var incoming float64
	if v, ok := fields[ag.Field]; ok {
		incoming, _ = v.AsFloat()
	}
	a, ok := st.aggs[key]
	if !ok {
		// The maps are copied: the caller hands the same ones to the sink,
		// which labels them further, and the window must not see that.
		a = &aggregator{
			start: ts, end: ts.Add(ag.every),
			labels: copyLabels(labels), fields: copyFields(fields),
			set: set, line: line, field: ag.Field, mode: ag.Mode,
		}
		st.aggs[key] = a
		switch ag.Mode {
		case "increment":
			a.value = incoming + 1
		default:
			a.value = incoming
		}
		return nil
	}
	switch ag.Mode {
	case "increment":
		a.value++
	case "sum":
		a.value += incoming
	case "max":
		if incoming > a.value {
			a.value = incoming
		}
	case "last":
		a.value = incoming
	}
	return nil
}

func (st *Stream) closeExpiredAggregators(now time.Time) []Result {
	if now.IsZero() {
		return nil
	}
	var out []Result
	var expired []string
	for k, a := range st.aggs {
		if !now.Before(a.end) {
			expired = append(expired, k)
		}
	}
	sort.Strings(expired)
	for _, k := range expired {
		out = append(out, st.aggs[k].emit())
		delete(st.aggs, k)
	}
	return out
}

func (a *aggregator) emit() Result {
	fields := map[string]model.Value{}
	for k, v := range a.fields {
		fields[k] = v
	}
	if a.value == float64(int64(a.value)) {
		fields[a.field] = model.Int(int64(a.value))
	} else {
		fields[a.field] = model.Float(a.value)
	}
	return Result{Set: a.set, TSMs: a.start.UnixMilli(), Labels: a.labels, Fields: fields, Line: a.line}
}

// trimToRune drops a trailing partial UTF-8 sequence left by cutting a
// record at a byte boundary. Without it a truncated capture becomes an
// invalid-UTF-8 label value, which the store rejects by name -- so an
// over-long record lost its whole sample rather than its tail.
func trimToRune(s string) string {
	for len(s) > 0 {
		r, size := utf8.DecodeLastRuneInString(s)
		if r != utf8.RuneError || size > 1 {
			return s
		}
		s = s[:len(s)-1]
	}
	return s
}

func copyLabels(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func copyFields(in map[string]model.Value) map[string]model.Value {
	out := make(map[string]model.Value, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
