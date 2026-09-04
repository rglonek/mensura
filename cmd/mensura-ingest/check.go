package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"time"

	"github.com/rglonek/mensura/internal/ingest"
	"github.com/rglonek/mensura/pkg/extract"
)

// checkSample runs a spec over a real file and reports what it matched,
// what it did not, and the first lines it could not handle. Unmatched
// lines are the single most useful spec-debugging output there is, so they
// are printed rather than counted silently.
func checkSample(spec *extract.Spec, path string, labels map[string]string, verbose bool) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	head := make([]byte, 64<<10)
	n, _ := f.ReadAt(head, 0)

	// The operator labels are passed exactly as processFile passes
	// i.cfg.Labels. Selecting with nil meant a profile chosen by
	// select.label_equals could never match here, so the tool whose job
	// is to predict what the import will do answered "no profile matched"
	// for a spec the import handles.
	profile := spec.SelectProfile(path, head[:n], labels, "")
	if profile == nil {
		return fmt.Errorf("no profile matched %s: add a select: rule, or a default profile", path)
	}
	fmt.Printf("\nsample %s matched profile %q\n", path, profile.Name)
	if found := spec.DiscoverIdentity(path, head[:n]); len(found) > 0 {
		fmt.Printf("discovered identity: %v\n", found)
	}

	stream, err := spec.NewStream(profile, extract.StreamOptions{RefTime: info.ModTime()})
	if err != nil {
		return err
	}
	perSet := map[string]int{}
	lines := 0
	// readRecord, not a bufio.Scanner: a Scanner fails the whole file
	// with ErrTooLong on one over-long line, so `check` used to abort on
	// a file the import would have read to the end -- truncating that
	// record and carrying on. The tool whose job is to predict the
	// import has to frame the way the import does.
	br := bufio.NewReader(f)
	for {
		rec, rerr := ingest.ReadRecord(br, 0)
		if len(rec.Line) > 0 || rec.Terminated {
			lines++
			results, _ := stream.Process(string(rec.Line))
			for _, r := range results {
				perSet[r.Set]++
				if verbose {
					fmt.Printf("  %s %s labels=%v fields=%v\n",
						time.UnixMilli(r.TSMs).UTC().Format(time.RFC3339Nano), r.Set, r.Labels, r.Fields)
				}
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			return rerr
		}
	}
	for _, r := range stream.Flush() {
		perSet[r.Set]++
	}

	st := stream.Stats
	fmt.Printf("\n%d line(s) read\n", lines)
	sets := make([]string, 0, len(perSet))
	for s := range perSet {
		sets = append(sets, s)
	}
	sort.Strings(sets)
	for _, s := range sets {
		fmt.Printf("  set %-20s %d sample(s)\n", s, perSet[s])
	}
	fmt.Printf("  unmatched lines:      %d (%.1f%%)\n", st.Unmatched, pct(st.Unmatched, int64(lines)))
	fmt.Printf("  timestamp failures:   %d (%.1f%%)\n", st.TSParseErrors, pct(st.TSParseErrors, int64(lines)))
	if st.Oversize > 0 {
		fmt.Printf("  oversize records:     %d\n", st.Oversize)
	}
	if st.Unjoined > 0 {
		fmt.Printf("  unjoined continuations: %d (matched continue_regex but no join rule)\n", st.Unjoined)
	}
	for i, l := range st.FirstUnmatched {
		if i == 0 {
			fmt.Println("\nfirst unmatched lines:")
		}
		fmt.Printf("  %s\n", l)
	}
	if st.Unmatched > 0 && float64(st.Unmatched)/float64(max64(int64(lines), 1)) > 0.5 {
		fmt.Println("\nmore than half the lines matched nothing: the spec is probably wrong for this file")
	}
	return nil
}

func pct(n, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(n) * 100 / float64(total)
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
