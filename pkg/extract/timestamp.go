package extract

import (
	"errors"
	"strconv"
	"time"
)

var (
	// ErrNoTimestamp means no declared format matched the record. The
	// record is counted and dropped: a wrong timestamp is worse than a
	// missing sample on an operational dashboard.
	ErrNoTimestamp = errors.New("extract: no timestamp format matched")
	// ErrNoMatch means no pattern claimed the record.
	ErrNoMatch = errors.New("extract: record matched no pattern")
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

func parseTimestamp(layout, text string, loc *time.Location, assumeYear int, prev time.Time) (time.Time, error) {
	switch layout {
	case "epoch_s", "epoch_ms", "epoch_us", "epoch_ns":
		n, err := strconv.ParseInt(trimSpace(text), 10, 64)
		if err != nil {
			return time.Time{}, err
		}
		switch layout {
		case "epoch_s":
			return time.UnixMilli(n * 1000).UTC(), nil
		case "epoch_ms":
			return time.UnixMilli(n).UTC(), nil
		case "epoch_us":
			return time.UnixMilli(n / 1000).UTC(), nil
		default:
			return time.UnixMilli(n / 1_000_000).UTC(), nil
		}
	}
	ts, err := time.ParseInLocation(layout, text, loc)
	if err != nil {
		return time.Time{}, err
	}
	// A year-less layout (classic syslog) parses into year 0. Resolve it
	// against the declared reference year, then roll back a year if the
	// result jumped implausibly far ahead of the previous record, which is
	// what a December-to-January boundary looks like.
	if ts.Year() == 0 {
		ts = ts.AddDate(assumeYear, 0, 0)
		if !prev.IsZero() && ts.Sub(prev) > 24*time.Hour {
			ts = ts.AddDate(-1, 0, 0)
		}
	}
	return ts, nil
}

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
