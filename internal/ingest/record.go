package ingest

import (
	"bufio"
	"errors"
	"io"
	"strconv"
)

// keyHint identifies one occurrence of a record so a set keyed by
// `offset` can keep every occurrence as its own row.
//
// docs/design/04-wire-protocol.md section 6 defines the hint as the stream
// identity plus the byte offset. Nothing used to supply it, so `offset`
// keying hashed set, timestamp and labels and *nothing else*: it dropped
// the field values that content keying hashes, and two records sharing a
// millisecond and a label set silently overwrote each other -- the exact
// opposite of what the scheme is for. pos is the byte offset the record
// started at (or a flush marker), and n separates the samples one record
// produced.
func keyHint(stream, pos string, n int) string {
	h := stream + "\x00" + pos
	if n != 0 {
		h += "\x00" + strconv.Itoa(n)
	}
	return h
}

// offsetPos renders a byte offset as a hint position.
func offsetPos(off int64) string { return strconv.FormatInt(off, 10) }

// flushPos renders the nth flush of a stream's buffered state as a hint
// position; a flush has no byte offset of its own.
func flushPos(seq int) string { return "flush:" + strconv.Itoa(seq) }

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
