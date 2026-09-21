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
//
// L005 is advisory for the reason L004 is: the stream-label set it
// compares against is the union of every `identity:` rule's captures, and
// a rule scoped by `match_path` does not apply to every stream, so a
// pattern in another profile may legitimately capture that name.
//
// L006 is the half of L005 where that escape does not exist. `host` and
// `source` are not discovered, they are *defaulted*: every acquisition
// path fills them in when nothing else has (the hostname and the file's
// base name locally, the peer address and the listener name on a
// receiver), so a pattern that captures either as a field produces a
// sample carrying one name as a label and as a field on every single
// record -- which store.rowFor refuses by name, for the life of the
// process, with nothing pointing back at the spec. A spec that can never
// store a row is a broken spec, which is what L001 is fatal for.
// L007 is advisory for a different reason again: it names what an
// aggregating pattern captures and the window cannot carry. That is
// almost always a spec the author should narrow, but it is not
// necessarily wrong -- a label outside `on:` whose value is functionally
// determined by the keys that *are* in `on:` is reported identically to
// one that varies, and this cannot tell them apart from the spec alone.
func (l Lint) Fatal() bool { return l.Code == "L001" || l.Code == "L006" }

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
		out = append(out, p.lintAggregateCaptures()...)
	}
	return out
}

// lintAggregateCaptures reports what an aggregating pattern captures that
// its windows cannot represent.
//
// A window is keyed on `on:` and carries one accumulated column. Two
// kinds of capture do not survive that:
//
//   - A *field* other than `aggregate.field`. The window has no single
//     value for it -- every record in the window may carry a different
//     one -- so emit() writes only the accumulated column and the capture
//     is discarded. It used to be carried forward from whichever record
//     opened the window, which drew a plausible per-window series of
//     numbers nothing had aggregated.
//   - A *label* that is not one of the `on:` keys. The window has to
//     carry some value for it, because the labels are the emitted
//     sample's identity, and the only one available is the opening
//     record's. Where the label really varies within a window, every row
//     is attributed to whichever value happened to arrive first.
//
// Both are decidable from the spec and invisible at run time: the row is
// well formed and the panel draws.
func (p *Profile) lintAggregateCaptures() []Lint {
	var out []Lint
	for _, pat := range p.Patterns {
		if pat.Aggregate == nil {
			continue
		}
		on := make(map[string]struct{}, len(pat.Aggregate.On))
		for _, k := range pat.Aggregate.On {
			on[k] = struct{}{}
		}
		var dropped, carried []string
		seen := map[string]struct{}{}
		names := patternExtractedNames(pat)
		// The columns a bucket set expands into are written straight
		// into the same field map, so they are discarded with the rest.
		if bs, ok := p.buckets[pat.BucketSet]; ok && pat.BucketSet != "" {
			for _, b := range bs.Buckets {
				names = append(names, b)
				if bs.Cumulative {
					names = append(names, b+"plus")
				}
			}
			if bs.Tail {
				names = append(names, tailField)
			}
		}
		for _, name := range names {
			if name == "" || name == "buckets" || name == "histogram" {
				continue
			}
			if name == pat.Aggregate.Field {
				continue // the accumulated column itself
			}
			if _, dup := seen[name]; dup {
				continue
			}
			seen[name] = struct{}{}
			if isDeclaredLabel(p, pat, name) {
				if _, keyed := on[name]; !keyed {
					carried = append(carried, name)
				}
				continue
			}
			dropped = append(dropped, name)
		}
		sort.Strings(dropped)
		sort.Strings(carried)
		if len(dropped) > 0 {
			out = append(out, Lint{
				Profile: p.Name, Code: "L007",
				Msg: fmt.Sprintf("pattern for set %q aggregates into %q, so the field(s) %s it also captures are discarded: a window has one value per `aggregate.field` and no single value for anything else -- write a second, non-aggregating pattern for them, or aggregate them too",
					pat.Set, pat.Aggregate.Field, strings.Join(quoteAll(dropped), ", ")),
			})
		}
		if len(carried) > 0 {
			out = append(out, Lint{
				Profile: p.Name, Code: "L007",
				Msg: fmt.Sprintf("pattern for set %q aggregates on %s, but also captures the label(s) %s; a window carries whichever value the record that opened it had, so every row is attributed to that one -- add them to `on:` if they vary within a window",
					pat.Set, strings.Join(quoteAll(pat.Aggregate.On), ", "), strings.Join(quoteAll(carried), ", ")),
			})
		}
	}
	return out
}

// quoteAll renders a list of names for a diagnostic.
func quoteAll(names []string) []string {
	out := make([]string, 0, len(names))
	for _, n := range names {
		out = append(out, fmt.Sprintf("%q", n))
	}
	return out
}

// alwaysAttached are the stream labels no spec can avoid. Every
// acquisition path fills them in when identity discovery and --label have
// not: the hostname and the file's base name for a file, the peer address
// and the listener name for a connection. A capture that collides with
// one of them therefore collides on every record, which is what makes
// L006 fatal where L005 is advisory.
var alwaysAttached = map[string]struct{}{"host": {}, "source": {}}

// identityLabels lists the label names the `identity:` rules can produce,
// plus the two the acquisition layer always attaches.
func (s *Spec) identityLabels() map[string]struct{} {
	out := map[string]struct{}{}
	for k := range alwaysAttached {
		out[k] = struct{}{}
	}
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
			// A capture the profile does not classify as a label is a
			// *field*, and the acquisition layer attaches the stream's
			// own labels to every sample it produces -- host and source
			// always, plus whatever `identity:` discovers. A field whose
			// name collides with one of those arrives at the store as a
			// label and a field on the same sample, which store.rowFor
			// refuses by name ("%q is both a label and a field"): every
			// record the pattern produces is rejected, one at a time, for
			// the life of the process, and the only trace is a line in
			// the store's log on the far side of the sink. It is decidable
			// here, from the spec alone.
			if _, ok := streamLabels[name]; ok {
				// Fatal for the two names nothing can withhold, advisory
				// for the rest. An `identity:` rule scoped by
				// `match_path` does not apply to every stream, so a
				// pattern in another profile may legitimately capture
				// its name; `host` and `source` have no such escape --
				// every acquisition path defaults them -- so a pattern
				// that captures one as a field has every record it
				// produces refused by the store.
				code := "L005"
				if _, always := alwaysAttached[name]; always {
					code = "L006"
				}
				out = append(out, Lint{
					Profile: p.Name, Code: code,
					Msg: fmt.Sprintf("pattern for set %q captures %q as a field, but %q is a stream label the acquisition layer attaches to every sample; the store refuses a sample that carries one name as both, so every record this pattern produces would be rejected -- declare it in `labels:` or capture it under another name",
						pat.Set, name, name),
				})
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
