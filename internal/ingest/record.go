package ingest

import (
	"bufio"
	"errors"
	"io"
)

// defaultMaxRecordBytes bounds one record on every acquisition path.
const defaultMaxRecordBytes = 1 << 20

// record is one framed line plus what it cost to read.
type record struct {
	// Line is the record without its terminator, truncated to the cap.
	Line []byte
	// Consumed is how many bytes of the stream the record occupied,
	// including the terminator and including anything discarded past the
	// cap. Byte offsets are checkpoints, so this must count the whole
	// record and not just the part that was kept.
	Consumed int
	// Terminated reports that a newline was seen. A false value at the
	// end of a stream is a partial line still being written.
	Terminated bool
	// Oversize reports that the record was longer than the cap and the
	// excess was discarded.
	Oversize bool
}

// readRecord reads one newline-terminated record, never buffering more
// than max bytes of it.
//
// This exists because the two acquisition paths used to disagree, in
// opposite and equally bad directions. Batch import used a bufio.Scanner
// capped at one megabyte, so a single longer line failed the scan with
// ErrTooLong and abandoned the rest of the file. Follow used ReadBytes
// with no cap at all, so one newline-free file matched by a glob was read
// entirely into memory -- and then again on every poll, because an
// unterminated read is held back for its newline.
//
// Reading through ReadSlice bounds the buffering to the reader's own
// buffer, and a record longer than the cap is truncated and reported
// rather than being allowed to fail or exhaust anything.
func readRecord(r *bufio.Reader, max int) (record, error) {
	if max <= 0 {
		max = defaultMaxRecordBytes
	}
	var rec record
	for {
		chunk, err := r.ReadSlice('\n')
		rec.Consumed += len(chunk)
		if n := max - len(rec.Line); n > 0 {
			if n > len(chunk) {
				n = len(chunk)
			}
			rec.Line = append(rec.Line, chunk[:n]...)
		} else if len(chunk) > 0 {
			rec.Oversize = true
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			// More of this record is coming; keep draining it.
			continue
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return rec, io.EOF
			}
			return rec, err
		}
		rec.Terminated = true
		// Drop the terminator, and a CRLF's carriage return with it.
		rec.Line = trimEOL(rec.Line)
		return rec, nil
	}
}

func trimEOL(b []byte) []byte {
	if n := len(b); n > 0 && b[n-1] == '\n' {
		b = b[:n-1]
	}
	if n := len(b); n > 0 && b[n-1] == '\r' {
		b = b[:n-1]
	}
	return b
}
