package extract

import (
	"fmt"
	"sort"
	"strings"
)

// Lint is one problem with a compiled spec that is not fatal on its own
// but almost certainly is not what the author meant.
//
// These live here rather than in `mensura-ingest check` because they are
// properties of the spec, and the tool whose job is to predict what an
// import will do must not reimplement the thing it is predicting.
type Lint struct {
	// Profile is the profile the finding belongs to, empty for a
	// document-level one.
	Profile string
	// Code is a stable short identifier, so a finding can be searched
	// for and asserted on.
	Code string
	Msg  string
}

func (l Lint) String() string {
	if l.Profile == "" {
		return l.Code + ": " + l.Msg
	}
	return fmt.Sprintf("%s: profile %s: %s", l.Code, l.Profile, l.Msg)
}

// Lint reports the spec problems that Compile deliberately does not
// refuse: a pattern that can never be reached, and a capture or a field
// declaration that does not resolve to anything.
//
// docs/design/03-extraction.md section 1 promises both checks. Neither
// existed, so `check` reported a spec good and the import then silently
// wrote nothing for a whole set -- which is the one outcome the tool is
// there to prevent.
//
// The spec must already have been compiled; Load and Parse both do that.
func (s *Spec) Lint() []Lint {
	var out []Lint
	for _, p := range s.Profiles {
		out = append(out, p.lintUnreachablePatterns()...)
		out = append(out, p.lintCaptures()...)
	}
	return out
}

// lintUnreachablePatterns reports a pattern that the matcher can never
// select.
//
// acMatcher.FirstIndex returns the *lowest* pattern index whose `search`
// literal occurs in the line, and process() then uses that one pattern
// and no other. So if an earlier pattern's literal is a substring of a
// later one's, every line the later pattern could claim contains the
// earlier literal too, and the later pattern is dead: its set is never
// written, and nothing at run time says so, because the line did match
// something.
//
// The empty literal is the extreme case of the same rule -- it occurs in
// every line -- and it shadows every pattern after it.
func (p *Profile) lintUnreachablePatterns() []Lint {
	var out []Lint
	for j, later := range p.Patterns {
		for i := 0; i < j; i++ {
			earlier := p.Patterns[i]
			if !strings.Contains(later.Search, earlier.Search) {
				continue
			}
			switch {
			case earlier.Search == "":
				out = append(out, Lint{
					Profile: p.Name, Code: "L001",
					Msg: fmt.Sprintf("pattern %d (set %q) can never be reached: pattern %d (set %q) has an empty `search`, which matches every record, and the first matching pattern wins",
						j+1, later.Set, i+1, earlier.Set),
				})
			default:
				out = append(out, Lint{
					Profile: p.Name, Code: "L001",
					Msg: fmt.Sprintf("pattern %d (set %q, search %q) can never be reached: pattern %d (set %q) searches for %q, which every one of those records also contains, and the first matching pattern wins -- put the more specific pattern first",
						j+1, later.Set, later.Search, i+1, earlier.Set, earlier.Search),
				})
			}
			break // one report per shadowed pattern is enough
		}
	}
	return out
}

// lintCaptures reports captures and field declarations that do not
// resolve: a capture that is neither a declared label nor a declared
// field, and a declared field that no pattern ever captures.
func (p *Profile) lintCaptures() []Lint {
	var out []Lint
	captured := map[string]struct{}{}
	for _, pat := range p.Patterns {
		var unresolved []string
		for _, name := range patternCaptureNames(pat) {
			if name == "" {
				continue
			}
			captured[name] = struct{}{}
			// The two reserved histogram group names are payloads, not
			// columns: expand() consumes them and they never reach a row.
			if name == "buckets" || name == "histogram" {
				continue
			}
			if _, ok := p.labelSet[name]; ok {
				continue
			}
			if _, ok := pat.labelSet[name]; ok {
				continue
			}
			if _, ok := p.Fields[name]; ok {
				continue
			}
			unresolved = append(unresolved, name)
		}
		// The bucket columns a bucket set expands into are captured by
		// the set, not by a regex.
		if pat.BucketSet != "" {
			if bs, ok := p.buckets[pat.BucketSet]; ok {
				if bs.TotalField != "" {
					// `total_field:` is a declaration of the capture's
					// role, so the capture is resolved even without a
					// `fields:` entry.
					unresolved = removeName(unresolved, bs.TotalField)
				}
				for _, b := range bs.Buckets {
					captured[b] = struct{}{}
					if bs.Cumulative {
						captured[b+"plus"] = struct{}{}
					}
				}
				if bs.Tail {
					captured[tailField] = struct{}{}
				}
			}
		}
		sort.Strings(unresolved)
		for _, name := range unresolved {
			out = append(out, Lint{
				Profile: p.Name, Code: "L002",
				Msg: fmt.Sprintf("pattern for set %q captures %q, which is declared neither as a label nor in `fields:`; it will be stored as an untyped gauge with no unit, no cadence and no limits",
					pat.Set, name),
			})
		}
	}
	var undeclared []string
	for name := range p.Fields {
		if _, ok := captured[name]; !ok {
			undeclared = append(undeclared, name)
		}
	}
	sort.Strings(undeclared)
	for _, name := range undeclared {
		out = append(out, Lint{
			Profile: p.Name, Code: "L003",
			Msg: fmt.Sprintf("field %q is declared in `fields:` but no pattern captures it, so its metadata reaches no column", name),
		})
	}
	var unusedLabels []string
	for name := range p.labelSet {
		if _, ok := captured[name]; !ok {
			unusedLabels = append(unusedLabels, name)
		}
	}
	sort.Strings(unusedLabels)
	for _, name := range unusedLabels {
		// A stream label supplied by identity discovery or by --label is
		// attached by the sink and never captured here, so this is only
		// reported when the name is not one of those either.
		if isStreamLabel(name) {
			continue
		}
		out = append(out, Lint{
			Profile: p.Name, Code: "L004",
			Msg: fmt.Sprintf("label %q is declared but no pattern captures it; if it comes from `identity:` or --label it is attached by the stream and needs no declaration here", name),
		})
	}
	return out
}

// removeName drops one entry from a small list of findings.
func removeName(names []string, drop string) []string {
	out := names[:0]
	for _, n := range names {
		if n != drop {
			out = append(out, n)
		}
	}
	return out
}

// isStreamLabel reports whether a name is one the acquisition layer
// attaches to every sample regardless of the patterns.
func isStreamLabel(name string) bool {
	return name == "host" || name == "source"
}
