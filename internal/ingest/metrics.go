package ingest

import (
	"fmt"
	"net/http"
)

// MetricsHandler serves the ingest counters Prometheus scrapes. The
// progress document and the _mensura_ingest set stop when the store is
// unreachable; this listener does not.
func MetricsHandler(p *Progress, s *Sink) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" && r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		snap := p.Snapshot()
		st := s.Snapshot()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "# HELP mensura_ingest_records_total Records framed.\n# TYPE mensura_ingest_records_total counter\nmensura_ingest_records_total %d\n", snap.Records)
		fmt.Fprintf(w, "# HELP mensura_ingest_samples_total Samples extracted.\n# TYPE mensura_ingest_samples_total counter\nmensura_ingest_samples_total %d\n", snap.Samples)
		fmt.Fprintf(w, "mensura_ingest_unmatched_lines_total %d\n", snap.UnmatchedLines)
		fmt.Fprintf(w, "mensura_ingest_timestamp_errors_total %d\n", snap.TSParseErrors)
		fmt.Fprintf(w, "mensura_ingest_extract_errors_total %d\n", snap.ExtractErrors)
		fmt.Fprintf(w, "# HELP mensura_ingest_lag_bytes Bytes followed files are behind their writers.\n# TYPE mensura_ingest_lag_bytes gauge\nmensura_ingest_lag_bytes %d\n", snap.LagBytes)
		fmt.Fprintf(w, "mensura_ingest_udp_dropped_total %d\n", snap.UDPDropped)
		fmt.Fprintf(w, "mensura_ingest_batches_sent_total %d\n", st.Sent)
		fmt.Fprintf(w, "mensura_ingest_samples_accepted_total %d\n", st.Accepted)
		fmt.Fprintf(w, "mensura_ingest_samples_rejected_total %d\n", st.Rejected)
		fmt.Fprintf(w, "mensura_ingest_batches_retried_total %d\n", st.Retried)
		fmt.Fprintf(w, "mensura_ingest_samples_dropped_total %d\n", st.Dropped)
		fmt.Fprintf(w, "# HELP mensura_ingest_buffered_samples Samples waiting for the store.\n# TYPE mensura_ingest_buffered_samples gauge\nmensura_ingest_buffered_samples %d\n", s.buffered())
	})
}
