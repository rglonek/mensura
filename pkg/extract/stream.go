package extract

import (
	"container/heap"
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
	// aggQ orders the open windows by the time they end, so expiry costs
	// only the windows that have expired. It used to be a scan of the
	// whole map on every matching record, and with a high-cardinality
	// aggregate.on key that map holds an entry per key per window: the
	// per-record cost grew with cardinality, on the hot path, per stream.
	aggQ aggQueue

	// mark is the caller-supplied position of the record currently being
	// processed -- a byte offset, for a caller that checkpoints one. It is
	// copied onto every buffer this stream opens so HeldFrom can say which
	// bytes are still only in memory.
	mark int64
	// holds lists the open aggregation windows in mark order, so the
	// oldest live window is at the front: no heap is needed, only a lazy
	// pop of entries whose window has since closed. Marks handed in by
	// the driver only ever increase, so holdWindow normally appends; the
	// one case that does not is a buffered multiline record, which is
	// processed under the mark of the line that opened it.
	holds []holdEntry

	Stats Stats

	maxUnmatchedSamples int
}

// holdEntry remembers where an aggregation window started. Windows close
// out of mark order only when a profile mixes several `every` widths,
// which the scan below tolerates.
//
// An entry outlives the window it names. A window that has closed and
// been emitted still bounds where a checkpoint may go: see HeldFrom.
type holdEntry struct {
	mark int64
	key  string
	a    *aggregator
}

// live reports whether a hold entry's window is still open, as opposed to
// closed and already emitted. The aggregator is compared by identity as
// well as by key, because a later window under the same key is a
// different window.
func (st *Stream) live(e holdEntry) bool {
	a, ok := st.aggs[e.key]
	return ok && a == e.a
}

type mlBuffer struct {
	line string
	ts   time.Time
	seen time.Time
	// mark is where the record that opened this buffer began.
	mark int64
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
	// hasValue reports whether any record has actually contributed a
	// value. It is not the same as "the window is open": a window opened
	// by a record that does not carry the field starts at zero, and for
	// `max` zero is a floor no negative reading can ever beat, so a
	// window of only negative values reported 0 -- a number nothing
	// measured, on a metric where negative values are the point.
	hasValue bool
	// mark is where the record that opened this window began.
	mark int64
	// lastMark is where the most recent record folded into this window
	// began. Together with mark it is the window's span in the stream,
	// which is what tells HeldFrom whether a checkpoint would land inside
	// it.
	lastMark int64
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

// Mark tags the record that Process is about to be handed with the
// position it started at, so HeldFrom can name it later. Callers that do
// not checkpoint byte offsets never call it and never read HeldFrom.
//
// Positions must not decrease within one stream: the hold list below is
// kept in mark order by appending to it.
func (st *Stream) Mark(pos int64) { st.mark = pos }

// HeldFrom reports the mark of the oldest record whose data this stream is
// still holding rather than having returned -- an open multiline buffer or
// an unfinished aggregation window -- and whether it is holding anything
// at all.
//
// It exists because a record that yields no samples is not the same as a
// record that has been dealt with. A driver that checkpoints byte offsets
// used to advance past both alike, so the bytes of a line that had merely
// opened a multiline block, or of one that had been folded into a window
// that had not closed yet, were recorded as delivered while their sample
// existed only in this struct. A crash in that window lost them with
// nothing saying so, and a rewind threw away the part of an open window
// that came from bytes already acknowledged.
func (st *Stream) HeldFrom() (int64, bool) {
	oldest, held := st.holdFloor()
	st.pruneHolds()
	return oldest, held
}

// holdFloor is the oldest position this stream still needs a replay to
// start at, and whether it needs one at all.
//
// It is the oldest open multiline buffer or aggregation window -- and
// then, crucially, it is pulled back past any *emitted* window whose span
// contains that position.
//
// The pull-back is not belt and braces. Windows for different aggregation
// keys interleave: key `a`'s window can close while key `b`'s, opened
// later, is still open. Without the pull-back the floor rises to `b`'s
// mark, which sits in the middle of the records that fed `a` -- correctly
// as far as `a` goes, whose sample has already been handed to the sink.
// But a replay from there does not rebuild `a`: those records open a *new*
// window at the wrong start timestamp, so the store gains a row nobody
// measured, and the record that should have opened the next real window is
// swallowed into it instead. A partial count appears and a real one
// disappears, both in silence, on every restart and every hole rewind.
//
// Pulling back to the emitted window's own start makes the replay rebuild
// it whole, so the duplicate collapses under a content-addressed key and
// the following window opens where it did the first time.
//
// One backward pass suffices: the list is in mark order, the floor only
// falls, and an entry whose mark is at or past the floor cannot contain
// it. The floor never falls *between* calls either -- a window's span only
// grows while it is open, and while it is open it pins the floor to its
// own mark -- which is what lets pruneHolds drop anything behind it.
func (st *Stream) holdFloor() (int64, bool) {
	oldest, held := int64(0), false
	for _, b := range st.multiline {
		if !held || b.mark < oldest {
			oldest, held = b.mark, true
		}
	}
	for _, e := range st.holds {
		if st.live(e) && (!held || e.mark < oldest) {
			oldest, held = e.mark, true
		}
	}
	if !held {
		return 0, false
	}
	for i := len(st.holds) - 1; i >= 0; i-- {
		e := st.holds[i]
		if e.mark >= oldest || st.live(e) {
			continue
		}
		if e.a.lastMark >= oldest {
			oldest = e.mark
		}
	}
	return oldest, true
}

// holdWindow records where an open aggregation window started, keeping
// the list in mark order.
//
// The insertion scan normally does nothing: Mark's own contract is that
// positions do not decrease, so a window opened by the record in hand
// belongs at the end. A buffered multiline record is the exception --
// it is processed under the mark of the line that opened it, which is
// behind the line that flushed it -- and HeldFrom reads only the first
// live entry, so an out-of-order append there would hide the older
// window and let a checkpoint advance past data that exists only inside
// this struct.
func (st *Stream) holdWindow(key string, a *aggregator) {
	e := holdEntry{mark: a.mark, key: key, a: a}
	i := len(st.holds)
	for i > 0 && st.holds[i-1].mark > e.mark {
		i--
	}
	st.holds = append(st.holds, holdEntry{})
	copy(st.holds[i+1:], st.holds[i:])
	st.holds[i] = e
	st.pruneHolds()
}

// pruneHolds drops entries that can no longer constrain a checkpoint.
//
// The floor never falls between calls, so an emitted window matters again
// only if a position at or past the current floor -- and past the
// window's own start -- can still fall inside its span. A window that
// absorbed only the record that opened it spans a single position and can
// never contain anything, which is every window on a driver that does not
// mark its records at all. When nothing is open the list goes entirely,
// since the driver may then acknowledge its read head, which is past
// every span there is.
//
// It is called where holds grow as well as where they are read, because
// only some drivers read them: HeldFrom exists for a caller that
// checkpoints byte offsets, and the receive path -- which runs for the
// life of the process, with one stream per peer -- never calls it. There
// the list grew by one entry, plus the aggregation key it retains, for
// every window ever opened: a one-minute `every` over a thousand keys is
// a million entries a day that nothing would ever look at.
//
// What survives is bounded by the windows whose spans reach the floor,
// which is the same order as the open set, so the threshold below still
// amortises the compaction rather than running it per window.
func (st *Stream) pruneHolds() {
	if len(st.holds) > 2*len(st.aggs)+16 {
		floor, held := st.holdFloor()
		if !held {
			st.holds = nil
			return
		}
		kept := st.holds[:0]
		for _, e := range st.holds {
			// An emitted window matters again only if some future floor
			// can land strictly inside its span. Floors never fall, so
			// that needs a position at or past the current one *and*
			// past the window's own start: a window that absorbed only
			// the record that opened it spans a single position, which
			// nothing can be inside.
			if st.live(e) || (e.a.lastMark >= floor && e.a.lastMark > e.mark) {
				kept = append(kept, e)
			}
		}
		st.holds = kept
	}
	if len(st.holds) == 0 {
		st.holds = nil // let the backing array go
	}
}

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
			var err error
			if buf, ok := st.multiline[m.StartContains]; ok {
				// The verdict on the record this line just closed is
				// returned, not discarded.
				//
				// A multiline record is almost always flushed by the next
				// start marker, and this call used to report success for
				// every one of them. The drivers turn that verdict into
				// the pipeline's counters -- Progress.UnmatchedLines,
				// TSParseErrors, ExtractErrors -- so a profile whose
				// joined records matched no pattern reported "0
				// unmatched" on the console, in the progress document and
				// in the _mensura_ingest set, while the stream's own
				// Stats (which only `check --sample` reads) counted every
				// one. The line in hand has been buffered rather than
				// judged, so it is owed no verdict of its own and this is
				// the only one there is to give.
				out, err = st.processBuffered(buf)
			}
			st.multiline[m.StartContains] = &mlBuffer{line: line, ts: ts, seen: time.Now(), mark: st.mark}
			return out, err
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
			out, _ := st.processBuffered(buf)
			return out, fmt.Errorf("extract: multiline record timestamps moved backwards")
		}
		for i := range m.Join {
			j := &m.Join[i]
			if g := j.re.FindStringSubmatch(line); len(g) > j.Capture {
				// Capped the way a single record is. Appending without a
				// bound let a stream of continuation lines grow one
				// string for as long as the idle timeout allowed, with
				// max_record_bytes capping only the lines going in. The
				// overflow is counted, so a truncated joined record is
				// not mistaken for one that arrived whole.
				if room := m.maxRecordBytes - len(buf.line); m.maxRecordBytes > 0 && len(g[j.Capture]) > room {
					if room > 0 {
						buf.line += trimToRune(g[j.Capture][:room])
					}
					st.Stats.Oversize++
				} else {
					buf.line += g[j.Capture]
				}
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
		r, _ := st.processBuffered(buf)
		out = append(out, r...)
	}
	// Only these are counted here. process() already counted everything
	// it returned, so adding len(out) on top double-counted every
	// flushed multiline record.
	for _, k := range st.aggKeysInEndOrder() {
		a := st.aggs[k]
		delete(st.aggs, k)
		out = append(out, a.emit())
		st.Stats.Samples++
	}
	st.aggQ = nil
	st.holds = nil
	return out
}

// aggKeysInEndOrder lists the open windows oldest end first, which is the
// order closeExpiredAggregators would have released them in. Ranging the
// map instead made a flush emit in whatever order Go felt like.
func (st *Stream) aggKeysInEndOrder() []string {
	keys := make([]string, 0, len(st.aggs))
	for _, e := range st.aggQ {
		if a, ok := st.aggs[e.key]; ok && a == e.a {
			keys = append(keys, e.key)
		}
	}
	sort.SliceStable(keys, func(i, j int) bool {
		ai, aj := st.aggs[keys[i]], st.aggs[keys[j]]
		if ai.end.Equal(aj.end) {
			return keys[i] < keys[j]
		}
		return ai.end.Before(aj.end)
	})
	return keys
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
		r, _ := st.processBuffered(buf)
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

// processBuffered runs a completed multiline record through the pattern
// stage under the mark of the line that *opened* it, not the mark of
// whatever record happened to flush it.
//
// st.mark is the position the driver last handed in, and an aggregation
// window records it so HeldFrom can hold a checkpoint back to the bytes
// the window was built from. A buffered record is flushed by a later
// line -- the next start marker, an idle timeout, a rotation -- so
// processing it under the live mark registered the window at a position
// past its own data: the checkpoint could then be acknowledged over
// bytes whose only copy was the still-open window, and a crash or a
// rewind lost them with nothing saying so.
func (st *Stream) processBuffered(buf *mlBuffer) ([]Result, error) {
	saved := st.mark
	st.mark = buf.mark
	out, err := st.process(buf.line, buf.ts)
	st.mark = saved
	return out, err
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
	// The window is identified by what it produces, not only by where it
	// goes: the destination set, the column the accumulator synthesises,
	// how it accumulates, and the label values it is keyed on.
	//
	// The set and the `on` values used to be the whole of it, so two
	// aggregating patterns writing one set with the same `on` keys shared
	// a single window -- and a window keeps the field and mode of
	// whichever pattern opened it. A profile with
	//
	//	patterns: [ {search: COUNT, aggregate: {field: hits, mode: increment, on: [op]}},
	//	            {search: LAT,   aggregate: {field: lat,  mode: max,       on: [op]}} ]
	//
	// emitted one row per key carrying `hits`, whose value was the
	// latency: the max fold ran against the counter's accumulator, and
	// `lat` was never written at all. Nothing reported it -- both
	// patterns matched, both records were counted, and the series that
	// vanished looks exactly like one the source never emitted.
	//
	// Two patterns that really do declare the same column with the same
	// mode still share a window, which is the one case where merging is
	// what the spec asks for.
	keyParts := make([]string, 0, len(ag.On)+3)
	keyParts = append(keyParts, set, ag.Field, ag.Mode)
	for _, on := range ag.On {
		v, ok := labels[on]
		if !ok {
			return fmt.Errorf("extract: aggregation key %q is not a declared label", on)
		}
		keyParts = append(keyParts, on+"="+v)
	}
	key := strings.Join(keyParts, "\x00")

	// The verdict AsFloat returns is honoured rather than discarded.
	//
	// It reports false for a value that is not a finite number, which is
	// exactly what model.Coerce produces from a log line spelling a field
	// "NaN", "Inf" or "+Infinity" -- strconv.ParseFloat accepts all
	// three. Folding one into an accumulator did not cost that record, it
	// cost the window: `sum` stays NaN for every later record, and a
	// window seeded with one never moves again because `incoming > NaN`
	// is false. The window's sample is then refused outright by the sink
	// as unencodable, so one junk line silently erased every record that
	// shared its window. It reports false for a non-numeric string too,
	// and summing that as zero is the same silent wrong answer one
	// magnitude smaller.
	//
	// Absence is not the same as junk: a record that simply does not
	// carry the field contributes nothing and is not an error, which is
	// what `increment` relies on and what a sparse `sum` source produces.
	var incoming float64
	var usable bool
	if v, ok := fields[ag.Field]; ok {
		if incoming, usable = v.AsFloat(); !usable && ag.Mode != "increment" {
			return fmt.Errorf("extract: aggregate field %q is %s, which is not a finite number, so it cannot be %s into a window",
				ag.Field, v.String(), ag.Mode)
		}
	}
	a, ok := st.aggs[key]
	if !ok {
		// The maps are copied: the caller hands the same ones to the sink,
		// which labels them further, and the window must not see that.
		a = &aggregator{
			start: ts, end: ts.Add(ag.every),
			labels: copyLabels(labels), fields: copyFields(fields),
			set: set, line: line, field: ag.Field, mode: ag.Mode,
			mark: st.mark, lastMark: st.mark,
		}
		st.aggs[key] = a
		heap.Push(&st.aggQ, aggEntry{key: key, end: a.end, a: a})
		st.holdWindow(key, a)
		switch ag.Mode {
		case "increment":
			// One occurrence, counted. Seeding with the captured value
			// plus one meant a pattern that also extracted the field it
			// counts started every window at that value, so the first
			// window of each key reported a number nothing had counted.
			a.value, a.hasValue = 1, true
		default:
			// A window opened by a record that carries no value starts at
			// zero, which is what a sum of nothing is -- but it is not a
			// reading, so `max` below still treats the next real value as
			// the first one rather than comparing it against that zero.
			if usable {
				a.value, a.hasValue = incoming, true
			}
		}
		return nil
	}
	// Every record that reaches an open window extends its span, whether
	// or not it moves the value: a replay that started inside the span
	// would absorb it into a different window, so the span is what
	// HeldFrom has to see.
	if st.mark > a.lastMark {
		a.lastMark = st.mark
	}
	if !usable {
		// Nothing to fold. `increment` counts the occurrence regardless;
		// the value modes leave the window exactly as it was.
		//
		// The window's own mode decides, not the record's. The key above
		// makes them the same thing, and reading it off the accumulator
		// is what keeps that true if the key ever changes again.
		if a.mode == "increment" {
			a.value++
		}
		return nil
	}
	switch a.mode {
	case "increment":
		a.value++
	case "sum":
		a.value += incoming
	case "max":
		if !a.hasValue || incoming > a.value {
			a.value = incoming
		}
	case "last":
		a.value = incoming
	}
	a.hasValue = true
	return nil
}

// closeExpiredAggregators emits and removes every window that has ended by
// now, oldest end first.
//
// The queue is what keeps this proportional to the number of expired
// windows rather than to the number of open ones: it is called on every
// matching record, and walking the whole map there made the per-record
// cost grow with the aggregation key's cardinality.
func (st *Stream) closeExpiredAggregators(now time.Time) []Result {
	if now.IsZero() {
		return nil
	}
	var out []Result
	for len(st.aggQ) > 0 {
		e := st.aggQ[0]
		if now.Before(e.end) {
			break
		}
		heap.Pop(&st.aggQ)
		// The map and the queue are updated together, so an entry whose
		// window is gone can only be one a future change forgot to
		// remove from both. Skipping it beats emitting it twice.
		a, ok := st.aggs[e.key]
		if !ok || a != e.a {
			continue
		}
		out = append(out, a.emit())
		delete(st.aggs, e.key)
	}
	return out
}

// aggEntry is one open window in the expiry queue. end is copied from the
// aggregator, which never moves it after the window opens.
type aggEntry struct {
	key string
	end time.Time
	a   *aggregator
}

// aggQueue is a min-heap of open windows ordered by end time, then by key
// so that windows ending together close in a stable order.
type aggQueue []aggEntry

func (q aggQueue) Len() int { return len(q) }
func (q aggQueue) Less(i, j int) bool {
	if q[i].end.Equal(q[j].end) {
		return q[i].key < q[j].key
	}
	return q[i].end.Before(q[j].end)
}
func (q aggQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *aggQueue) Push(x any)   { *q = append(*q, x.(aggEntry)) }
func (q *aggQueue) Pop() any {
	old := *q
	n := len(old)
	e := old[n-1]
	*q = old[:n-1]
	return e
}

// maxExactInt is the largest magnitude a float64 carries as an exact
// integer. Past it float-to-int conversion is undefined by the Go spec --
// amd64 yields the indefinite value and arm64 saturates -- so the test
// below is range-checked before it is made rather than relying on either.
const maxExactInt = 1 << 53

func (a *aggregator) emit() Result {
	fields := map[string]model.Value{}
	for k, v := range a.fields {
		fields[k] = v
	}
	if a.value >= -maxExactInt && a.value <= maxExactInt && a.value == float64(int64(a.value)) {
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
