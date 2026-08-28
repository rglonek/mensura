package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/rglonek/mensura/pkg/extract"
)

// checkSample runs a spec over a real file and reports what it matched,
// what it did not, and the first lines it could not handle. Unmatched
// lines are the single most useful spec-debugging output there is, so they
// are printed rather than counted silently.
func checkSample(spec *extract.Spec, path string, verbose bool) error {
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

	profile := spec.SelectProfile(path, head[:n], nil, "")
	if profile == nil {
		return fmt.Errorf("no profile matched %s: add a select: rule, or a default profile", path)
	}
	fmt.Printf("\nsample %s matched profile %q\n", path, profile.Name)
	if labels := spec.DiscoverIdentity(path, head[:n]); len(labels) > 0 {
		fmt.Printf("discovered identity: %v\n", labels)
	}

	stream, err := spec.NewStream(profile, extract.StreamOptions{RefTime: info.ModTime()})
	if err != nil {
		return err
	}
	perSet := map[string]int{}
	lines := 0
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		lines++
		results, _ := stream.Process(sc.Text())
		for _, r := range results {
			perSet[r.Set]++
			if verbose {
				fmt.Printf("  %s %s labels=%v fields=%v\n",
					time.UnixMilli(r.TSMs).UTC().Format(time.RFC3339Nano), r.Set, r.Labels, r.Fields)
			}
		}
	}
	for _, r := range stream.Flush() {
		perSet[r.Set]++
	}
	if err := sc.Err(); err != nil {
		return err
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
