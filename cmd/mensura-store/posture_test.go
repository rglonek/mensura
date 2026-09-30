package main

import "testing"

// The startup posture check is the store's only defence against an open
// write API, and it had no test at all: isLoopbackAddr decides whether an
// unauthenticated listener may bind, and every mistake it can make is a
// write endpoint on a shared network.
//
// A name is not resolved and not trusted, because "myhost:9631" may well
// resolve to a routable address -- so anything that is not a loopback IP
// literal counts as public. "localhost" is the one exception, and it is
// not a guess: RFC 6761 reserves the name and requires every resolver to
// answer it with a loopback address.
func TestIsLoopbackAddr(t *testing.T) {
	for _, tc := range []struct {
		addr    string
		want    bool
		wantErr bool
	}{
		{"127.0.0.1:9631", true, false},
		{"127.0.0.2:9631", true, false},
		{"[::1]:9631", true, false},
		{"[::ffff:127.0.0.1]:9631", true, false},
		{"localhost:9631", true, false},
		{":9631", false, false},        // every interface
		{"0.0.0.0:9631", false, false}, // every interface, spelled out
		{"[::]:9631", false, false},
		{"10.0.0.1:9631", false, false},
		{"myhost:9631", false, false}, // a name may resolve anywhere
		{"LOCALHOST:9631", false, false},
		{"127.0.0.1", false, true}, // no port at all
	} {
		got, err := isLoopbackAddr(tc.addr)
		if (err != nil) != tc.wantErr {
			t.Errorf("%q: err = %v, wantErr %v", tc.addr, err, tc.wantErr)
			continue
		}
		if err == nil && got != tc.want {
			t.Errorf("%q: loopback = %v, want %v", tc.addr, got, tc.want)
		}
	}
}

// Convenience on a laptop must not become an open write API, and the
// debug listener -- which carries no authentication in any mode -- may
// only ever be bound to loopback.
func TestAuthPostureRefusesAnOpenListener(t *testing.T) {
	public := func(mode string, spec func(*fileConfig)) error {
		cfg := &fileConfig{}
		cfg.Auth.Mode = mode
		cfg.Listen.Write.Addr = "127.0.0.1:9631"
		spec(cfg)
		return checkAuthPosture(cfg)
	}
	if err := public("", func(c *fileConfig) { c.Listen.Write.Addr = "0.0.0.0:9631" }); err == nil {
		t.Error("an unauthenticated write listener on every interface was accepted")
	}
	if err := public("none", func(c *fileConfig) { c.Listen.Query.Addr = "10.0.0.1:9632" }); err == nil {
		t.Error("an unauthenticated query listener on a routable address was accepted")
	}
	if err := public("none", func(c *fileConfig) { c.Listen.Metrics.Addr = "10.0.0.1:9633" }); err == nil {
		t.Error("an unauthenticated metrics listener on a routable address was accepted")
	}
	// The debug listener is checked whatever the auth mode: it has none of
	// its own.
	if err := public("bearer", func(c *fileConfig) { c.Listen.Debug.Addr = "10.0.0.1:9634" }); err == nil {
		t.Error("a non-loopback debug listener was accepted under bearer auth")
	}
	// Bearer auth is what makes a routable bind legitimate.
	if err := public("bearer", func(c *fileConfig) { c.Listen.Write.Addr = "0.0.0.0:9631" }); err != nil {
		t.Errorf("bearer auth on a public address was refused: %v", err)
	}
	// And the documented loopback posture still starts.
	if err := public("none", func(c *fileConfig) {
		c.Listen.Query.Addr = "localhost:9632"
		c.Listen.Debug.Addr = "127.0.0.1:9633"
		c.Listen.Metrics.Addr = "[::1]:9634"
	}); err != nil {
		t.Errorf("the documented loopback posture was refused: %v", err)
	}
}
