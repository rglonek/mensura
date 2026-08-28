package extract

// acMatcher is an Aho-Corasick automaton over every pattern's literal
// `search` string. It replaces N substring scans per line with one
// O(len(line)) pass, and preserves first-match-wins exactly: FirstIndex
// returns the smallest pattern index whose literal occurs anywhere in the
// line, which is what a linear scan would have chosen.
type acMatcher struct {
	next  []map[byte]int32
	fail  []int32
	out   []int32 // smallest pattern index ending at this node, or -1
	empty []int32 // patterns with an empty search literal: always candidates
}

func newACMatcher(patterns []string) *acMatcher {
	m := &acMatcher{
		next: []map[byte]int32{{}},
		fail: []int32{0},
		out:  []int32{-1},
	}
	for pi, p := range patterns {
		if p == "" {
			m.empty = append(m.empty, int32(pi))
			continue
		}
		node := int32(0)
		for i := 0; i < len(p); i++ {
			c := p[i]
			nxt, ok := m.next[node][c]
			if !ok {
				nxt = int32(len(m.next))
				m.next[node][c] = nxt
				m.next = append(m.next, map[byte]int32{})
				m.fail = append(m.fail, 0)
				m.out = append(m.out, -1)
			}
			node = nxt
		}
		if m.out[node] == -1 || m.out[node] > int32(pi) {
			m.out[node] = int32(pi)
		}
	}
	// Breadth-first failure links.
	queue := make([]int32, 0, len(m.next))
	for c, n := range m.next[0] {
		_ = c
		m.fail[n] = 0
		queue = append(queue, n)
	}
	for i := 0; i < len(queue); i++ {
		cur := queue[i]
		for c, n := range m.next[cur] {
			f := m.fail[cur]
			for f != 0 {
				if _, ok := m.next[f][c]; ok {
					break
				}
				f = m.fail[f]
			}
			if nf, ok := m.next[f][c]; ok && nf != n {
				m.fail[n] = nf
			} else {
				m.fail[n] = 0
			}
			// Inherit the best output from the failure chain so a match on
			// a suffix still reports the earliest pattern.
			if o := m.out[m.fail[n]]; o != -1 && (m.out[n] == -1 || o < m.out[n]) {
				m.out[n] = o
			}
			queue = append(queue, n)
		}
	}
	return m
}

// FirstIndex reports the lowest pattern index whose literal appears in s,
// or -1 when none does.
func (m *acMatcher) FirstIndex(s string) int {
	best := int32(-1)
	for _, e := range m.empty {
		if best == -1 || e < best {
			best = e
		}
	}
	node := int32(0)
	for i := 0; i < len(s); i++ {
		c := s[i]
		for {
			if n, ok := m.next[node][c]; ok {
				node = n
				break
			}
			if node == 0 {
				break
			}
			node = m.fail[node]
		}
		if o := m.out[node]; o != -1 && (best == -1 || o < best) {
			best = o
			if best == 0 {
				return 0
			}
		}
	}
	return int(best)
}
