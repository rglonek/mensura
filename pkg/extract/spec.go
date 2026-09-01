// Package extract turns text into samples according to a declarative
// spec. Nothing here knows any product's log format: the format lives in
// the spec file, which is data. See docs/design/03-extraction.md.
package extract

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/rglonek/mensura/pkg/model"
	"github.com/rglonek/mensura/pkg/mql"
	"gopkg.in/yaml.v3"
)

// Spec is a whole extraction document.
type Spec struct {
	Version  int               `yaml:"version"`
	Include  []string          `yaml:"include"`
	Defaults Defaults          `yaml:"defaults"`
	Identity []IdentityRule    `yaml:"identity"`
	Profiles []*Profile        `yaml:"profiles"`
	Sets     map[string]SetOpt `yaml:"sets"`

	path string
}

// merge fills in what this Defaults block left unset from an included
// one. Label lists are unioned, because a label declared by a base is
// still a label.
func (d *Defaults) merge(other Defaults) {
	if d.Timestamp.Timezone == "" {
		d.Timestamp.Timezone = other.Timestamp.Timezone
	}
	if d.Timestamp.AssumeYear == "" {
		d.Timestamp.AssumeYear = other.Timestamp.AssumeYear
	}
	have := make(map[string]struct{}, len(d.Labels))
	for _, l := range d.Labels {
		have[l] = struct{}{}
	}
	for _, l := range other.Labels {
		if _, ok := have[l]; !ok {
			have[l] = struct{}{}
			d.Labels = append(d.Labels, l)
		}
	}
}

type Defaults struct {
	Timestamp struct {
		Timezone   string `yaml:"timezone"`
		AssumeYear string `yaml:"assume_year"` // file-mtime | now | <year>
	} `yaml:"timestamp"`
	Labels []string `yaml:"labels"`
}

// IdentityRule discovers stream labels from a path or from the head of a
// file. Content beats path, and both are beaten by an explicit operator
// declaration, because a file name is the least trustworthy thing about a
// log: rotation, unpacking and bundling all rewrite it.
type IdentityRule struct {
	MatchPath string `yaml:"match_path"`
	ScanLines int    `yaml:"scan_lines"`
	Regex     string `yaml:"regex"`

	matchPath *regexp.Regexp
	regex     *regexp.Regexp
}

type SetOpt struct {
	Retention string `yaml:"retention"`
	Shard     string `yaml:"shard"`
	Key       string `yaml:"key"` // content | offset

	retentionMs *int64
	shardMs     *int64
}

// RetentionMs and ShardMs return the compiled durations, or nil when the
// spec did not declare one.
func (o SetOpt) RetentionMs() *int64 { return o.retentionMs }
func (o SetOpt) ShardMs() *int64     { return o.shardMs }

// Profile is one log dialect: how to find timestamps, how to frame
// records, and what to extract.
type Profile struct {
	Name       string               `yaml:"name"`
	Select     Selector             `yaml:"select"`
	Timestamp  TimestampSpec        `yaml:"timestamp"`
	Framing    Framing              `yaml:"framing"`
	Labels     []string             `yaml:"labels"`
	Fields     map[string]FieldSpec `yaml:"fields"`
	Patterns   []*Pattern           `yaml:"patterns"`
	BucketSets []*BucketSet         `yaml:"bucket_sets"`

	labelSet map[string]struct{}
	matcher  *acMatcher
	buckets  map[string]*BucketSet
}

// Selector decides which profile parses a stream. Dialect selection and
// stream identity are deliberately separate concepts: using an identity
// label to pick the parser is the mistake this design avoids.
type Selector struct {
	PathGlob        []string          `yaml:"path_glob"`
	ContentContains []string          `yaml:"content_contains"`
	LabelEquals     map[string]string `yaml:"label_equals"`
	Listener        string            `yaml:"listener"`
	ScanBytes       int               `yaml:"scan_bytes"`
}

type TimestampSpec struct {
	Formats      []TimestampFormat `yaml:"formats"`
	Anchor       string            `yaml:"anchor"` // prefix | anywhere
	Strip        bool              `yaml:"strip"`
	OnParseError string            `yaml:"on_parse_error"` // count | drop-stream | fail
}

type TimestampFormat struct {
	Layout string `yaml:"layout"` // Go reference layout, or epoch_s|epoch_ms|epoch_us|epoch_ns
	Regex  string `yaml:"regex"`

	re *regexp.Regexp
}

// FieldSpec is the metadata that flows from the spec into the catalogue
// and on into the query builder's defaults.
type FieldSpec struct {
	Kind        string `yaml:"kind"`
	Unit        string `yaml:"unit"`
	UnitHint    string `yaml:"unit_hint"`
	Description string `yaml:"description"`
	MaxInterval string `yaml:"max_interval"`
	Limits      *struct {
		Min *float64 `yaml:"min"`
		Max *float64 `yaml:"max"`
	} `yaml:"limits"`

	maxIntervalMs int64
}

type Framing struct {
	Record         string       `yaml:"record"` // line | json
	MaxRecordBytes int          `yaml:"max_record_bytes"`
	Multiline      []*Multiline `yaml:"multiline"`
}

type Multiline struct {
	StartContains string `yaml:"start_contains"`
	ContinueRegex string `yaml:"continue_regex"`
	Join          []struct {
		Regex   string `yaml:"regex"`
		Capture int    `yaml:"capture"`

		re *regexp.Regexp
	} `yaml:"join"`
	IdleTimeout string `yaml:"idle_timeout"`

	continueRe  *regexp.Regexp
	idleTimeout time.Duration
}

// Pattern is one extraction rule.
type Pattern struct {
	Set              string            `yaml:"set"`
	Search           string            `yaml:"search"`
	Replace          []ReplaceRule     `yaml:"replace"`
	Extract          []string          `yaml:"extract"`
	Route            []RouteRule       `yaml:"route"`
	Labels           []string          `yaml:"labels"`
	DefaultValues    map[string]string `yaml:"default_values"`
	StoreStreamLabel string            `yaml:"store_stream_label"`
	BucketSet        string            `yaml:"bucket_set"`
	Aggregate        *Aggregate        `yaml:"aggregate"`

	extract  []*regexp.Regexp
	labelSet map[string]struct{}
}

type ReplaceRule struct {
	Regex string `yaml:"regex"`
	Sub   string `yaml:"sub"`

	re *regexp.Regexp
}

type RouteRule struct {
	Regex string `yaml:"regex"`
	Set   string `yaml:"set"`

	re *regexp.Regexp
}

// Aggregate counts or sums occurrences over a window instead of storing
// one row per record. It is lossy on purpose, and `check` reports how
// lossy.
type Aggregate struct {
	Every string   `yaml:"every"`
	On    []string `yaml:"on"`
	Field string   `yaml:"field"`
	Mode  string   `yaml:"mode"` // increment | sum | max | last

	every time.Duration
}

// BucketSet declares a histogram layout, so the query layer can render a
// heatmap with real numeric edges instead of ordinal buckets.
type BucketSet struct {
	Name       string   `yaml:"name"`
	Parse      string   `yaml:"parse"` // paren_pairs | csv | key_value | json_object
	Buckets    []string `yaml:"buckets"`
	Edges      string   `yaml:"edges"` // pow2 | linear:<step> | explicit:[…]
	EdgeUnit   string   `yaml:"edge_unit"`
	TotalField string   `yaml:"total_field"`
	Cumulative bool     `yaml:"cumulative"`
	Tail       bool     `yaml:"tail"`

	edges []float64
}

// maxPow2Buckets bounds a pow2 bucket set. Beyond this the doubling edge
// exceeds what a float64 can represent exactly, so the labels would stop
// meaning anything.
const maxPow2Buckets = 64

// Load reads and compiles a spec, following includes relative to the
// including file.
func Load(path string) (*Spec, error) {
	s, err := load(path, map[string]bool{}, map[string]bool{})
	if err != nil {
		return nil, err
	}
	if err := s.Compile(); err != nil {
		return nil, err
	}
	return s, nil
}

// load reads one spec and its includes. stack holds the chain currently
// being loaded, so a genuine cycle is caught; done holds everything
// already loaded, so a diamond — two profiles including one common base —
// is skipped rather than mistaken for a cycle.
func load(path string, stack map[string]bool, done map[string]bool) (*Spec, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	if stack[abs] {
		return nil, fmt.Errorf("extract: include cycle at %s", abs)
	}
	if done[abs] {
		return &Spec{path: abs}, nil
	}
	stack[abs] = true
	defer delete(stack, abs)
	b, err := os.ReadFile(abs)
	if err != nil {
		return nil, err
	}
	var s Spec
	dec := yaml.NewDecoder(strings.NewReader(string(b)))
	dec.KnownFields(true) // a typo must fail loudly, not vanish
	if err := dec.Decode(&s); err != nil {
		return nil, fmt.Errorf("extract: %s: %w", path, err)
	}
	s.path = abs
	for _, inc := range s.Include {
		p := inc
		if !filepath.IsAbs(p) {
			p = filepath.Join(filepath.Dir(abs), p)
		}
		sub, err := load(p, stack, done)
		if err != nil {
			return nil, err
		}
		if sub.Version != 0 && sub.Version != 1 {
			return nil, fmt.Errorf("extract: %s: unsupported spec version %d (expected 1)", p, sub.Version)
		}
		s.Profiles = append(s.Profiles, sub.Profiles...)
		s.Identity = append(s.Identity, sub.Identity...)
		// Defaults travel with an include too. They used to be dropped,
		// so a base spec holding the timezone, the year assumption or the
		// shared label list was read, compiled and silently ignored: the
		// including file's own defaults (usually none, meaning UTC) won
		// by default rather than by choice. The includer still wins where
		// it declared something itself.
		s.Defaults.merge(sub.Defaults)
		if s.Sets == nil {
			s.Sets = map[string]SetOpt{}
		}
		for k, v := range sub.Sets {
			if _, ok := s.Sets[k]; !ok {
				s.Sets[k] = v
			}
		}
	}
	done[abs] = true
	return &s, nil
}

// Parse compiles a spec from bytes, which is what tests and the receive
// path use.
func Parse(b []byte) (*Spec, error) {
	var s Spec
	dec := yaml.NewDecoder(strings.NewReader(string(b)))
	dec.KnownFields(true)
	if err := dec.Decode(&s); err != nil {
		return nil, err
	}
	if err := s.Compile(); err != nil {
		return nil, err
	}
	return &s, nil
}

// Compile validates the spec and builds every regex and automaton once.
func (s *Spec) Compile() error {
	if s.Version != 1 {
		return fmt.Errorf("extract: unsupported spec version %d (expected 1)", s.Version)
	}
	for i := range s.Identity {
		r := &s.Identity[i]
		var err error
		if r.MatchPath != "" {
			if r.matchPath, err = regexp.Compile(r.MatchPath); err != nil {
				return fmt.Errorf("extract: identity match_path: %w", err)
			}
		}
		if r.Regex != "" {
			if r.regex, err = regexp.Compile(r.Regex); err != nil {
				return fmt.Errorf("extract: identity regex: %w", err)
			}
		}
		if r.ScanLines == 0 {
			r.ScanLines = 500
		}
	}
	// The `sets:` block is compiled here so a bad duration or key scheme
	// is a spec error rather than something the store discovers later.
	for name, opt := range s.Sets {
		if err := model.ValidateSetName(name); err != nil {
			return fmt.Errorf("extract: sets: %w", err)
		}
		// A range check, not only a syntax check: ParseDuration accepts a
		// leading sign so that Print -> Parse round-trips an unvalidated
		// AST, so "-5s" here parsed cleanly and was carried all the way to
		// the store, which refused it on every single write.
		if opt.Retention != "" {
			ms, err := mql.ParseDuration(opt.Retention)
			if err != nil {
				return fmt.Errorf("extract: set %s retention: %w", name, err)
			}
			if ms < 0 {
				return fmt.Errorf("extract: set %s retention %q must not be negative", name, opt.Retention)
			}
			opt.retentionMs = &ms
		}
		if opt.Shard != "" {
			ms, err := mql.ParseDuration(opt.Shard)
			if err != nil {
				return fmt.Errorf("extract: set %s shard: %w", name, err)
			}
			if ms <= 0 {
				return fmt.Errorf("extract: set %s shard %q must be positive", name, opt.Shard)
			}
			opt.shardMs = &ms
		}
		switch opt.Key {
		case "", "content", "offset":
		default:
			return fmt.Errorf("extract: set %s key %q: expected content or offset", name, opt.Key)
		}
		s.Sets[name] = opt
	}
	names := map[string]bool{}
	for _, p := range s.Profiles {
		if p.Name == "" {
			return fmt.Errorf("extract: every profile needs a name")
		}
		if names[p.Name] {
			return fmt.Errorf("extract: duplicate profile name %q", p.Name)
		}
		names[p.Name] = true
		if err := p.compile(s); err != nil {
			return fmt.Errorf("extract: profile %s: %w", p.Name, err)
		}
	}
	return nil
}

func (p *Profile) compile(s *Spec) error {
	p.labelSet = map[string]struct{}{}
	for _, l := range append(append([]string{}, s.Defaults.Labels...), p.Labels...) {
		p.labelSet[l] = struct{}{}
	}
	if len(p.Timestamp.Formats) == 0 {
		return fmt.Errorf("no timestamp formats declared")
	}
	for i := range p.Timestamp.Formats {
		f := &p.Timestamp.Formats[i]
		if f.Regex == "" {
			return fmt.Errorf("timestamp format %q needs a regex", f.Layout)
		}
		re, err := regexp.Compile(f.Regex)
		if err != nil {
			return fmt.Errorf("timestamp regex: %w", err)
		}
		f.re = re
	}
	switch p.Timestamp.Anchor {
	case "":
		p.Timestamp.Anchor = "prefix"
	case "prefix", "anywhere":
	default:
		return fmt.Errorf("timestamp anchor %q: expected prefix or anywhere", p.Timestamp.Anchor)
	}
	// `count` is what the code does. The other two values documented in
	// 03-extraction.md are not implemented, and a declaration that does
	// nothing is worse than one that is rejected -- the same principle
	// that refuses store_stream_label and listen.*.tls.client_ca. An
	// operator must not be able to believe a stream is being dropped or
	// the process failed when neither happens.
	switch p.Timestamp.OnParseError {
	case "", "count":
		p.Timestamp.OnParseError = "count"
	case "drop-stream", "fail":
		return fmt.Errorf("timestamp on_parse_error %q is not implemented; only `count` is, and it is the default", p.Timestamp.OnParseError)
	default:
		return fmt.Errorf("timestamp on_parse_error %q: expected count", p.Timestamp.OnParseError)
	}
	switch p.Framing.Record {
	case "", "line":
		p.Framing.Record = "line"
	case "json":
		return fmt.Errorf("framing record: json is not implemented; records are framed by line, and a JSON record would be parsed as one")
	default:
		return fmt.Errorf("framing record %q: expected line", p.Framing.Record)
	}
	if p.Framing.MaxRecordBytes == 0 {
		p.Framing.MaxRecordBytes = 1 << 20
	}
	for _, m := range p.Framing.Multiline {
		var err error
		// An empty start_contains is silently self-defeating:
		// strings.Contains(line, "") is true for every line, so every
		// record opens a new multiline buffer, no continuation is ever
		// joined, and each record is held back until the next one arrives
		// -- or until the idle flush, on a quiet stream. Nothing reports
		// it; the profile simply lags and never multilines.
		if m.StartContains == "" {
			return fmt.Errorf("multiline needs a non-empty start_contains")
		}
		if m.ContinueRegex != "" {
			if m.continueRe, err = regexp.Compile(m.ContinueRegex); err != nil {
				return fmt.Errorf("multiline continue_regex: %w", err)
			}
		}
		for i := range m.Join {
			if m.Join[i].re, err = regexp.Compile(m.Join[i].Regex); err != nil {
				return fmt.Errorf("multiline join regex: %w", err)
			}
		}
		if m.IdleTimeout != "" {
			if m.idleTimeout, err = time.ParseDuration(m.IdleTimeout); err != nil {
				return fmt.Errorf("multiline idle_timeout: %w", err)
			}
		} else {
			m.idleTimeout = 30 * time.Second
		}
	}
	p.buckets = map[string]*BucketSet{}
	for _, bs := range p.BucketSets {
		if err := bs.compile(); err != nil {
			return err
		}
		// Bucket names become field names on the row, and expand()
		// derives "<bucket>plus" from them. Checking them here names the
		// offending bucket; leaving it to the store turns a spec typo
		// into a per-sample rejection with no pointer back to the spec.
		for _, b := range bs.Buckets {
			if err := model.ValidateFieldName(b); err != nil {
				return fmt.Errorf("bucket set %s: %w", bs.Name, err)
			}
			if bs.Cumulative {
				if err := model.ValidateFieldName(b + "plus"); err != nil {
					return fmt.Errorf("bucket set %s: cumulative column: %w", bs.Name, err)
				}
			}
		}
		p.buckets[bs.Name] = bs
	}
	for name := range p.Fields {
		if err := model.ValidateFieldName(name); err != nil {
			return err
		}
		fs := p.Fields[name]
		if fs.MaxInterval != "" {
			ms, err := mql.ParseDuration(fs.MaxInterval)
			if err != nil {
				return fmt.Errorf("field %s: %w", name, err)
			}
			if ms <= 0 {
				return fmt.Errorf("field %s: max_interval %q must be positive", name, fs.MaxInterval)
			}
			fs.maxIntervalMs = ms
		}
		p.Fields[name] = fs
	}

	searches := make([]string, 0, len(p.Patterns))
	for _, pat := range p.Patterns {
		if err := model.ValidateSetName(pat.Set); err != nil {
			return err
		}
		if model.IsReserved(pat.Set) {
			return fmt.Errorf("set %q uses the reserved prefix", pat.Set)
		}
		pat.labelSet = map[string]struct{}{}
		for _, l := range pat.Labels {
			pat.labelSet[l] = struct{}{}
		}
		for i := range pat.Replace {
			re, err := regexp.Compile(pat.Replace[i].Regex)
			if err != nil {
				return fmt.Errorf("replace regex: %w", err)
			}
			pat.Replace[i].re = re
		}
		for _, ex := range pat.Extract {
			re, err := regexp.Compile(ex)
			if err != nil {
				return fmt.Errorf("extract regex: %w", err)
			}
			pat.extract = append(pat.extract, re)
		}
		for i := range pat.Route {
			re, err := regexp.Compile(pat.Route[i].Regex)
			if err != nil {
				return fmt.Errorf("route regex: %w", err)
			}
			pat.Route[i].re = re
			if pat.Route[i].Set == "" {
				pat.Route[i].Set = pat.Set
			}
		}
		if len(pat.extract) == 0 && len(pat.Route) == 0 {
			return fmt.Errorf("pattern for set %q has neither extract nor route", pat.Set)
		}
		if pat.StoreStreamLabel != "" {
			// The key is accepted by the YAML decoder but nothing acts on
			// it. Failing is better than letting an operator believe a
			// label is being stored when it is not.
			return fmt.Errorf("pattern for set %q sets store_stream_label, which is not implemented; remove it", pat.Set)
		}
		if pat.BucketSet != "" {
			if _, ok := p.buckets[pat.BucketSet]; !ok {
				return fmt.Errorf("pattern references unknown bucket set %q", pat.BucketSet)
			}
		}
		if pat.Aggregate != nil {
			d, err := time.ParseDuration(pat.Aggregate.Every)
			if err != nil {
				return fmt.Errorf("aggregate every: %w", err)
			}
			pat.Aggregate.every = d
			if d <= 0 {
				return fmt.Errorf("aggregate every %q must be positive", pat.Aggregate.Every)
			}
			if pat.Aggregate.Field == "" {
				return fmt.Errorf("aggregate needs a field")
			}
			if pat.Aggregate.Mode == "" {
				pat.Aggregate.Mode = "increment"
			}
			// An unrecognised mode used to compile. The accumulator's
			// switch has no default, so the window kept whichever value
			// the first record carried and ignored every later one:
			// `mode: avg` produced a flat, entirely plausible series
			// with no error anywhere.
			switch pat.Aggregate.Mode {
			case "increment", "sum", "max", "last":
			default:
				return fmt.Errorf("aggregate mode %q: expected increment, sum, max or last", pat.Aggregate.Mode)
			}
		}
		searches = append(searches, pat.Search)
	}
	p.matcher = newACMatcher(searches)
	return nil
}

func (b *BucketSet) compile() error {
	if b.Name == "" || len(b.Buckets) == 0 {
		return fmt.Errorf("bucket set needs a name and buckets")
	}
	if b.Parse == "" {
		b.Parse = "paren_pairs"
	}
	b.edges = make([]float64, len(b.Buckets))
	switch {
	case b.Edges == "" || b.Edges == "pow2":
		// The common HDR-style layout: the first three buckets are 0, 1, 2
		// and each later bucket doubles.
		if len(b.Buckets) > maxPow2Buckets {
			return fmt.Errorf("bucket set %s: %d buckets exceeds the %d that a pow2 layout can label",
				b.Name, len(b.Buckets), maxPow2Buckets)
		}
		for i := range b.Buckets {
			switch i {
			case 0:
				b.edges[i] = 0
			case 1:
				b.edges[i] = 1
			default:
				// math.Ldexp rather than a shift: 1<<62 is the last value
				// an int64 shift can hold, and silently wrapping to a
				// negative edge would mislabel the whole heatmap.
				b.edges[i] = math.Ldexp(1, i-1)
			}
		}
	case strings.HasPrefix(b.Edges, "linear:"):
		var step float64
		if _, err := fmt.Sscanf(b.Edges[len("linear:"):], "%g", &step); err != nil {
			return fmt.Errorf("bucket set %s: bad linear step: %w", b.Name, err)
		}
		for i := range b.Buckets {
			b.edges[i] = float64(i) * step
		}
	case strings.HasPrefix(b.Edges, "explicit:"):
		raw := strings.Trim(b.Edges[len("explicit:"):], "[] ")
		parts := strings.Split(raw, ",")
		if len(parts) != len(b.Buckets) {
			return fmt.Errorf("bucket set %s: %d explicit edges for %d buckets", b.Name, len(parts), len(b.Buckets))
		}
		for i, p := range parts {
			var v float64
			if _, err := fmt.Sscanf(strings.TrimSpace(p), "%g", &v); err != nil {
				return fmt.Errorf("bucket set %s: bad edge %q", b.Name, p)
			}
			b.edges[i] = v
		}
	default:
		return fmt.Errorf("bucket set %s: unknown edges %q", b.Name, b.Edges)
	}
	return nil
}

// EdgeValues exposes the numeric lower bound of each bucket.
func (b *BucketSet) EdgeValues() []float64 { return b.edges }

// Profile returns a profile by name.
func (s *Spec) Profile(name string) *Profile {
	for _, p := range s.Profiles {
		if p.Name == name {
			return p
		}
	}
	return nil
}

// SelectProfile binds a stream to exactly one profile: the first selector
// that matches. A stream that matches nothing is reported rather than
// silently dropped.
func (s *Spec) SelectProfile(path string, head []byte, labels map[string]string, listener string) *Profile {
	for _, p := range s.Profiles {
		if p.matches(path, head, labels, listener) {
			return p
		}
	}
	return nil
}

func (p *Profile) matches(path string, head []byte, labels map[string]string, listener string) bool {
	sel := p.Select
	empty := len(sel.PathGlob) == 0 && len(sel.ContentContains) == 0 && len(sel.LabelEquals) == 0 && sel.Listener == ""
	if empty {
		return true
	}
	if sel.Listener != "" && sel.Listener != listener {
		return false
	}
	if len(sel.PathGlob) > 0 {
		ok := false
		base := filepath.Base(path)
		for _, g := range sel.PathGlob {
			if m, _ := filepath.Match(g, path); m {
				ok = true
				break
			}
			if m, _ := filepath.Match(g, base); m {
				ok = true
				break
			}
			if m, _ := filepath.Match(g, filepath.ToSlash(path)); m {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	if len(sel.ContentContains) > 0 {
		n := sel.ScanBytes
		if n <= 0 || n > len(head) {
			n = len(head)
		}
		h := string(head[:n])
		ok := false
		for _, c := range sel.ContentContains {
			if strings.Contains(h, c) {
				ok = true
				break
			}
		}
		if !ok {
			return false
		}
	}
	for k, v := range sel.LabelEquals {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// DiscoverIdentity applies the identity rules to a path and a file head,
// returning the labels they yield.
func (s *Spec) DiscoverIdentity(path string, head []byte) map[string]string {
	out := map[string]string{}
	for i := range s.Identity {
		r := &s.Identity[i]
		if r.matchPath != nil {
			if m := r.matchPath.FindStringSubmatch(filepath.ToSlash(path)); m != nil {
				collectNamed(r.matchPath, m, out)
			}
		}
		if r.regex != nil {
			lines := strings.SplitN(string(head), "\n", r.ScanLines+1)
			if len(lines) > r.ScanLines {
				lines = lines[:r.ScanLines]
			}
			for _, ln := range lines {
				if m := r.regex.FindStringSubmatch(ln); m != nil {
					collectNamed(r.regex, m, out)
					break
				}
			}
		}
	}
	return out
}

func collectNamed(re *regexp.Regexp, m []string, into map[string]string) {
	for i, name := range re.SubexpNames() {
		if i == 0 || name == "" || i >= len(m) || m[i] == "" {
			continue
		}
		into[name] = m[i]
	}
}

// SetDeclaration is what a profile promises about one destination set:
// the metadata of the fields that set actually carries, and any bucket
// sets its patterns populate. Metadata is declared per set because the
// catalogue is per set, and a profile may write several.
type SetDeclaration struct {
	Set        string
	Fields     map[string]FieldSpec
	BucketSets []*BucketSet
}

// Declarations resolves the profile's field metadata onto the sets its
// patterns write. A field is attributed to a set when a pattern writing
// that set captures it.
func (p *Profile) Declarations() []SetDeclaration {
	bySet := map[string]*SetDeclaration{}
	get := func(set string) *SetDeclaration {
		d, ok := bySet[set]
		if !ok {
			d = &SetDeclaration{Set: set, Fields: map[string]FieldSpec{}}
			bySet[set] = d
		}
		return d
	}
	for _, pat := range p.Patterns {
		sets := []string{pat.Set}
		for _, r := range pat.Route {
			sets = append(sets, r.Set)
		}
		var captures []string
		for _, re := range pat.extract {
			captures = append(captures, re.SubexpNames()...)
		}
		for i := range pat.Route {
			captures = append(captures, pat.Route[i].re.SubexpNames()...)
		}
		for k := range pat.DefaultValues {
			captures = append(captures, k)
		}
		if pat.Aggregate != nil {
			captures = append(captures, pat.Aggregate.Field)
		}
		for _, set := range sets {
			d := get(set)
			for _, name := range captures {
				if name == "" {
					continue
				}
				if fs, ok := p.Fields[name]; ok {
					d.Fields[name] = fs
				}
			}
			if pat.BucketSet != "" {
				if bs, ok := p.buckets[pat.BucketSet]; ok {
					d.BucketSets = append(d.BucketSets, bs)
				}
			}
		}
	}
	out := make([]SetDeclaration, 0, len(bySet))
	for _, d := range bySet {
		out = append(out, *d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Set < out[j].Set })
	return out
}

// FieldMeta exposes the declared metadata for one field.
func (p *Profile) FieldMeta(name string) (FieldSpec, bool) {
	f, ok := p.Fields[name]
	return f, ok
}

// MaxIntervalMs is the declared cadence in milliseconds, or 0.
func (f FieldSpec) MaxIntervalMs() int64 { return f.maxIntervalMs }
