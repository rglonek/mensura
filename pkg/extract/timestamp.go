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
// offset at which the record body starts.
//
// The index of the format that matched is cached on the stream and tried
// first next time, so the steady state is one regex match per record.
func (st *Stream) scanTimestamp(line string) (time.Time, int, error) {
	formats := st.profile.Timestamp.Formats
	if st.tsIdx >= 0 {
		if ts, off, err := st.tryFormat(st.tsIdx, line); err == nil {
			return ts, off, nil
		}
	}
	for i := range formats {
		if i == st.tsIdx {
			continue
		}
		if ts, off, err := st.tryFormat(i, line); err == nil {
			st.tsIdx = i
			return ts, off, nil
		}
	}
	return time.Time{}, 0, ErrNoTimestamp
}

func (st *Stream) tryFormat(i int, line string) (time.Time, int, error) {
	f := st.profile.Timestamp.Formats[i]
	loc := f.re.FindStringIndex(line)
	if loc == nil {
		return time.Time{}, 0, ErrNoTimestamp
	}
	if st.profile.Timestamp.Anchor == "prefix" && loc[0] != 0 {
		return time.Time{}, 0, ErrNoTimestamp
	}
	text := line[loc[0]:loc[1]]
	ts, err := parseTimestamp(f.Layout, text, st.loc, st.assumeYear, st.lastTS)
	if err != nil {
		return time.Time{}, 0, err
	}
	off := 0
	if st.profile.Timestamp.Strip {
		off = loc[1]
	}
	return ts, off, nil
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

func parseTimestamp(layout, text string, loc *time.Location, assumeYear int, prev time.Time) (time.Time, error) {
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
	// against the declared reference year, then roll back a year only for
	// the shape a December-to-January wrap actually has: a jump of most
	// of a year forward. A plain multi-day gap — a weekly cron log, a
	// host that was off over a weekend — is ordinary and must not be
	// thrown a year into the past.
	if ts.Year() == 0 {
		ts = ts.AddDate(assumeYear, 0, 0)
		if !prev.IsZero() && ts.Sub(prev) > yearWrapThreshold {
			ts = ts.AddDate(-1, 0, 0)
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
