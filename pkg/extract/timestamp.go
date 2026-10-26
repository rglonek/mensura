package extract

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/rglonek/mensura/pkg/model"
)

var (
	// ErrNoTimestamp means no declared format matched the record. The
	// record is counted and dropped: a wrong timestamp is worse than a
	// missing sample on an operational dashboard.
	ErrNoTimestamp = errors.New("extract: no timestamp format matched")
	// ErrNoMatch means no pattern claimed the record.
	ErrNoMatch = errors.New("extract: record matched no pattern")
	// ErrNoJoin means a line matched a multiline continue_regex but no
	// join rule captured anything from it, so it contributed nothing.
	ErrNoJoin = errors.New("extract: continuation line matched no join rule")
)

// scanTimestamp finds the timestamp in a record and returns it plus the
// record body: the original line, or the line with only the matched
// timestamp removed when `strip: true`.
//
// The index of the format that matched is cached on the stream and tried
// first next time, so the steady state is one regex match per record.
func (st *Stream) scanTimestamp(line string) (time.Time, string, error) {
	formats := st.profile.Timestamp.Formats
	if st.tsIdx >= 0 {
		if ts, body, err := st.tryFormat(st.tsIdx, line); err == nil {
			return ts, body, nil
		}
	}
	for i := range formats {
		if i == st.tsIdx {
			continue
		}
		if ts, body, err := st.tryFormat(i, line); err == nil {
			st.tsIdx = i
			return ts, body, nil
		}
	}
	return time.Time{}, "", ErrNoTimestamp
}

func (st *Stream) tryFormat(i int, line string) (time.Time, string, error) {
	f := st.profile.Timestamp.Formats[i]
	loc := f.re.FindStringIndex(line)
	if loc == nil {
		return time.Time{}, "", ErrNoTimestamp
	}
	if st.profile.Timestamp.Anchor == "prefix" && loc[0] != 0 {
		return time.Time{}, "", ErrNoTimestamp
	}
	text := line[loc[0]:loc[1]]
	ts, err := parseTimestamp(f.Layout, text, st.loc, st.assumeYear, st.lastTS, st.refTime)
	if err != nil {
		return time.Time{}, "", err
	}
	// The resolved year is the one subsequent year-less records should
	// start from. Without this a follow that crossed New Year would wrap
	// every January record against a frozen assumeYear of the previous
	// year, and a six-month gap after the wrap would stop wrapping.
	if y := ts.Year(); y != 0 {
		st.assumeYear = y
	}
	body := line
	if st.profile.Timestamp.Strip {
		// Only the match is removed. Slicing from loc[1] also dropped
		// everything *before* the timestamp, so `anchor: anywhere` plus
		// `strip: true` turned "web1 app[42]: 2024-01-01T00:00:00Z n=5"
		// into " n=5" and the pattern that searched for "app[" never
		// saw it.
		body = line[:loc[0]] + line[loc[1]:]
	}
	return ts, body, nil
}

// epochMillis converts an epoch count in a declared unit to milliseconds.
//
// The seconds form is range-checked rather than multiplied on trust: the
// realistic mistake is a spec declaring epoch_s against a source emitting
// nanoseconds, and n*1000 on a nanosecond value overflows int64 and wraps
// to a plausible-looking timestamp -- past model.MaxTSMs, which exists to
// catch exactly that mismatch, and sometimes back inside it. A record the
// declared unit cannot describe is reported as a timestamp failure, which
// is counted and dropped, rather than stored at a fabricated time.
//
// The sub-millisecond units floor rather than truncate, so a pre-epoch
// timestamp lands in the millisecond it belongs to instead of the one
// after it.
func epochMillis(layout string, n int64) (int64, error) {
	switch layout {
	case "epoch_s":
		if n > model.MaxTSMs/1000 || n < -(model.MaxTSMs/1000) {
			return 0, fmt.Errorf("%w: %d seconds is beyond the representable range; a value this large is usually epoch milliseconds, microseconds or nanoseconds declared as epoch_s", ErrNoTimestamp, n)
		}
		return n * 1000, nil
	case "epoch_ms":
		return n, nil
	case "epoch_us":
		return floorDiv(n, 1000), nil
	default:
		return floorDiv(n, 1_000_000), nil
	}
}

// floorDiv divides towards negative infinity, because Go's / truncates
// towards zero and would round a pre-epoch timestamp up.
func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

func parseTimestamp(layout, text string, loc *time.Location, assumeYear int, prev, ref time.Time) (time.Time, error) {
	switch layout {
	case "epoch_s", "epoch_ms", "epoch_us", "epoch_ns":
		n, err := strconv.ParseInt(trimSpace(text), 10, 64)
		if err != nil {
			return time.Time{}, err
		}
		ms, err := epochMillis(layout, n)
		if err != nil {
			return time.Time{}, err
		}
		return time.UnixMilli(ms).UTC(), nil
	}
	ts, err := time.ParseInLocation(layout, text, loc)
	if err != nil {
		return time.Time{}, err
	}
	// A year-less layout (classic syslog) parses into year 0. Resolve it
	// against the declared reference year, then wrap if the result is
	// more than half a year away from the previous record (or, for the
	// first record of a stream, from the reference time).
	//
	// The decrement — a jump of most of a year *forward* — is the
	// December line of a file whose mtime is in January: applying the
	// assumed year puts it eleven months ahead, so it is last December.
	// The increment is the other half of the same wrap, and it was
	// missing: a follow or a receive that stayed open across New Year
	// saw "Jan  1" after "Dec 31" and dated it as last January, because
	// that landing is in the *past* of the previous record and the
	// decrement never fired. A plain multi-day gap — a weekly cron log,
	// a host that was off over a weekend — is ordinary and must not be
	// thrown a year in either direction.
	if ts.Year() == 0 {
		ts = ts.AddDate(assumeYear, 0, 0)
		anchor := prev
		if anchor.IsZero() {
			anchor = ref
		}
		if !anchor.IsZero() {
			switch {
			case ts.Sub(anchor) > yearWrapThreshold:
				ts = ts.AddDate(-1, 0, 0)
			case anchor.Sub(ts) > yearWrapThreshold:
				ts = ts.AddDate(1, 0, 0)
			}
		}
	}
	return ts, nil
}

// yearWrapThreshold is how far ahead of the previous record a year-less
// timestamp must land before it is read as last year's date rather than a
// gap in the log. Half a year splits the two cases cleanly: a real wrap
// lands ~11 months ahead, a gap almost never does.
const yearWrapThreshold = 183 * 24 * time.Hour

func trimSpace(s string) string {
	i, j := 0, len(s)
	for i < j && (s[i] == ' ' || s[i] == '\t') {
		i++
	}
	for j > i && (s[j-1] == ' ' || s[j-1] == '\t') {
		j--
	}
	return s[i:j]
}
