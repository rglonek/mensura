package wire

import (
	"net/http"
	"testing"
	"time"
)

// TestRetryAfterIsCapped pins the bound on a header this client does not
// control.
//
// Mensura's own store always answers `Retry-After: 1`, but a reverse
// proxy, a load balancer or a service mesh in front of it commonly answers
// a 503 with minutes or hours. The value used to be taken as written in
// two places at once: Client.Write slept it out inside one request while
// holding the sink's delivery lock, and Sink.retryHoldFor then held
// delivery for the same interval -- so a 3600-second header stopped the
// ingester for an hour, filling its buffer to MaxBufferedSamples and then
// dropping the excess.
func TestRetryAfterIsCapped(t *testing.T) {
	cases := []struct {
		name   string
		header string
		want   time.Duration
		exact  bool
	}{
		{name: "absent", header: "", want: 0, exact: true},
		{name: "short delta seconds", header: "2", want: 2 * time.Second, exact: true},
		{name: "negative", header: "-5", want: 0, exact: true},
		{name: "long delta seconds", header: "3600", want: MaxRetryAfter, exact: true},
		{name: "unparseable", header: "soon", want: 0, exact: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := &http.Response{Header: http.Header{}}
			if tc.header != "" {
				resp.Header.Set("Retry-After", tc.header)
			}
			if got := retryAfter(resp); got != tc.want {
				t.Fatalf("Retry-After %q gave %s, want %s", tc.header, got, tc.want)
			}
		})
	}

	// The HTTP-date form is capped too, and a date in the past is no wait
	// at all rather than a negative one.
	resp := &http.Response{Header: http.Header{}}
	resp.Header.Set("Retry-After", time.Now().Add(24*time.Hour).UTC().Format(http.TimeFormat))
	if got := retryAfter(resp); got != MaxRetryAfter {
		t.Fatalf("a date a day out gave %s, want %s", got, MaxRetryAfter)
	}
	resp.Header.Set("Retry-After", time.Now().Add(-time.Hour).UTC().Format(http.TimeFormat))
	if got := retryAfter(resp); got != 0 {
		t.Fatalf("a date in the past gave %s, want 0", got)
	}
}
