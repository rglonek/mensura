package extract

import (
	"fmt"
	"regexp"
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

// Fatal reports whether a finding describes a spec that cannot do what it
// says, as opposed to one that works but declares more than it uses.
//
// The distinction is the difference between a pipeline that stops and one
// that ships. L001 is a pattern the matcher can never select, so a whole
// destination set is never written: that is a broken spec. The rest are
// advisory -- a capture with no `fields:` entry still lands, as an untyped
// gauge, and a declared label that no pattern captures is exactly what
// `identity:` and --label produce. `check` used to exit non-zero on all
// four alike, so declaring the operator label the README's own example
// passes (`--label dc=eu-west-1`) in `defaults.labels` failed the build --
// with a message that itself said the label needs no declaration.
func (l Lint) Fatal() bool { return l.Code == "L001" }

// Fatal reports whether any finding in a set is fatal.
func Fatal(lints []Lint) bool {
	for _, l := range lints {
		if l.Fatal() {
			return true
		}
	}
	return false
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
	// The names `identity:` attaches are resolved once for the whole
	// document: they arrive on the stream rather than off a record, so a
	// profile declaring one is not declaring something unused. Deriving
	// them from the rules beats the two hard-coded names that used to
	// stand in for the whole idea.
	stream := s.identityLabels()
	var out []Lint
	for _, p := range s.Profiles {
		out = append(out, p.lintUnreachablePatterns()...)
		out = append(out, p.lintCaptures(stream)...)
	}
	return out
}

// identityLabels lists the label names the `identity:` rules can produce,
// plus the two the acquisition layer always attaches.
func (s *Spec) identityLabels() map[string]struct{} {
	out := map[string]struct{}{"host": {}, "source": {}}
	for i := range s.Identity {
		r := &s.Identity[i]
		for _, re := range []*regexp.Regexp{r.matchPath, r.regex} {
			if re == nil {
				continue
			}
			for _, name := range re.SubexpNames() {
				if name != "" {
					out[name] = struct{}{}
				}
			}
		}
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
func (p *Profile) lintCaptures(streamLabels map[string]struct{}) []Lint {
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
		if _, ok := streamLabels[name]; ok {
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
