// Package engine is the embedded, single-process, sparse-column store:
// a Pebble LSM with a covering range index on one numeric column per set.
// See docs/design/05-storage.md.
package engine

import (
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
)

// NoBlockCache is the sentinel for Options.CacheBytes meaning "allocate no
// block cache at all"; zero means "unset, use the default".
const NoBlockCache int64 = -1

// BytesPerSyncDisabled disables Pebble's periodic sync_file_range, which on
// a network filesystem is a synchronous COMMIT round trip per call.
const BytesPerSyncDisabled = -1

// Options configure a store. Numeric zero always means "leave the default",
// so a partially-filled Options is safe to pass.
type Options struct {
	Path string

	// CacheBytes sizes the shared block cache: 0 default (1 GiB), >0 exact,
	// NoBlockCache to disable.
	CacheBytes int64

	// MemTableSizeBytes is the write buffer per memtable. Large buffers keep
	// a batched writer running at memory speed through an ingest burst.
	MemTableSizeBytes uint64

	// MemTableStopWritesThreshold caps queued memtables before writers block.
	MemTableStopWritesThreshold int

	// MaxConcurrentCompactions bounds simultaneous compactions.
	MaxConcurrentCompactions int

	MaxOpenFiles int

	// BlockSize is the target uncompressed SSTable data block size. The hot
	// path is range scans where successive reads share a block, so larger
	// blocks are close to free on the read side.
	BlockSize int

	// TargetFileSizeL0 bounds the SSTables a memtable flush produces. On a
	// network filesystem, small targets turn one flush into hundreds of
	// files, each paying metadata round trips.
	TargetFileSizeL0 int64

	// BytesPerSync: 0 leaves Pebble's 512 KiB cadence, BytesPerSyncDisabled
	// turns it off, >0 sets it.
	BytesPerSync int

	LBaseMaxBytes         int64
	L0StopWritesThreshold int

	// EnableBloomFilter installs a 10-bits-per-key filter on every level,
	// which makes negative point lookups effectively free.
	EnableBloomFilter bool

	// Compression selects a per-level profile: "", "snappy", "zstd",
	// "balanced" (snappy on the upper levels, zstd where the bytes settle),
	// or "none".
	Compression string

	// EnableWAL and SyncWrites set the durability posture. Batch import can
	// run with both off because the source files are the truth; follow and
	// receive ingest have no replayable source and want the WAL.
	EnableWAL  bool
	SyncWrites bool

	Logger *log.Logger
}

// DefaultOptions returns the tuned defaults documented in
// docs/design/05-storage.md section 9.
func DefaultOptions() Options {
	return Options{
		CacheBytes:                  1024 << 20,
		MemTableSizeBytes:           256 << 20,
		MemTableStopWritesThreshold: 4,
		MaxConcurrentCompactions:    4,
		BlockSize:                   32 << 10,
		Compression:                 "balanced",
	}
}

func (o *Options) applyDefaults() {
	if o.CacheBytes == 0 {
		o.CacheBytes = 1024 << 20
	}
	if o.MemTableSizeBytes == 0 {
		o.MemTableSizeBytes = 256 << 20
	}
	if o.MemTableStopWritesThreshold == 0 {
		o.MemTableStopWritesThreshold = 4
	}
	if o.MaxConcurrentCompactions == 0 {
		o.MaxConcurrentCompactions = 4
	}
	if o.BlockSize == 0 {
		o.BlockSize = 32 << 10
	}
	if o.Logger == nil {
		o.Logger = log.New(os.Stderr, "mensura-engine ", log.LstdFlags)
	}
}

const numLevels = 7

// pebbleOptions translates our knobs into Pebble's. Every translation that
// is not one-to-one is commented, because these are the values that decide
// whether an ingest burst lands at disk speed or stalls.
func (o *Options) pebbleOptions() *pebble.Options {
	po := &pebble.Options{
		MemTableSize:                o.MemTableSizeBytes,
		MemTableStopWritesThreshold: o.MemTableStopWritesThreshold,
		L0CompactionThreshold:       4,
		DisableWAL:                  !o.EnableWAL,
		Logger:                      &pebbleLogger{l: o.Logger},
	}
	po.Experimental.MaxWriterConcurrency = 2
	maxCompactions := o.MaxConcurrentCompactions
	po.MaxConcurrentCompactions = func() int { return maxCompactions }
	if o.CacheBytes > 0 {
		// Pebble's cache is reference counted and Open takes its own
		// reference, so the one held here is released by the caller once
		// the DB is open. Dropping it on the floor leaks the whole cache
		// for the lifetime of the process.
		po.Cache = pebble.NewCache(o.CacheBytes)
	}
	if o.MaxOpenFiles > 0 {
		po.MaxOpenFiles = o.MaxOpenFiles
	}
	if o.BytesPerSync == BytesPerSyncDisabled {
		po.BytesPerSync = 0
	} else if o.BytesPerSync > 0 {
		po.BytesPerSync = o.BytesPerSync
	}
	if o.LBaseMaxBytes > 0 {
		po.LBaseMaxBytes = o.LBaseMaxBytes
	}
	if o.L0StopWritesThreshold > 0 {
		po.L0StopWritesThreshold = o.L0StopWritesThreshold
	}

	po.Levels = make([]pebble.LevelOptions, numLevels)
	target := o.TargetFileSizeL0
	if target <= 0 {
		target = 2 << 20
	}
	for i := range po.Levels {
		l := &po.Levels[i]
		l.BlockSize = o.BlockSize
		l.IndexBlockSize = 256 << 10
		// Higher levels double the target file size, which is Pebble's own
		// convention; we only move the starting point.
		l.TargetFileSize = target << uint(i)
		l.Compression = compressionFor(o.Compression, i)
		if o.EnableBloomFilter {
			l.FilterPolicy = bloom.FilterPolicy(10)
			l.FilterType = pebble.TableFilter
		}
		l.EnsureDefaults()
	}
	return po
}

// compressionFor maps a profile name and a level index onto a codec.
// "balanced" spends cheap CPU on the upper levels, where flushes and minor
// compactions live, and pays for a tighter ratio on the bottom levels,
// where the bulk of the bytes settle.
func compressionFor(profile string, level int) pebble.Compression {
	switch strings.ToLower(profile) {
	case "none":
		return pebble.NoCompression
	case "zstd":
		return pebble.ZstdCompression
	case "balanced":
		if level >= numLevels-2 {
			return pebble.ZstdCompression
		}
		return pebble.SnappyCompression
	case "", "default", "snappy":
		return pebble.SnappyCompression
	}
	return pebble.SnappyCompression
}

// fatalFlushTimeout bounds the last-chance flush below. Pebble may be
// holding its own locks when it calls Fatalf, so a flush that cannot
// complete must not stop the process from dying.
const fatalFlushTimeout = 5 * time.Second

type pebbleLogger struct {
	l *log.Logger
	// onFatal is installed by Open once the DB exists. It is read from
	// whichever goroutine pebble reports the fault on, so it is atomic.
	onFatal atomic.Pointer[func() error]
}

// setFatalHook records what to run before the process is torn down.
func (p *pebbleLogger) setFatalHook(f func() error) { p.onFatal.Store(&f) }

func (p *pebbleLogger) Infof(format string, args ...any)  { p.l.Printf(format, args...) }
func (p *pebbleLogger) Errorf(format string, args ...any) { p.l.Printf("ERROR "+format, args...) }

// Fatalf exits, as pebble requires, but flushes on the way out.
//
// It used to forward to log.Fatalf, so the process left through os.Exit
// with every deferred Close skipped -- and Close is what performs the
// explicit Flush that makes "a graceful stop is durable in every profile"
// true. Under durability: batch there is no WAL behind it, so everything
// still in a memtable was discarded on the one path where the process is
// already dying because something went wrong.
func (p *pebbleLogger) Fatalf(format string, args ...any) {
	p.l.Printf("FATAL "+format, args...)
	if h := p.onFatal.Load(); h != nil {
		done := make(chan struct{})
		go func() {
			defer close(done)
			if err := (*h)(); err != nil {
				p.l.Printf("ERROR flushing after a fatal engine error: %v", err)
			}
		}()
		select {
		case <-done:
		case <-time.After(fatalFlushTimeout):
			p.l.Printf("ERROR flush after a fatal engine error did not finish within %s; exiting anyway", fatalFlushTimeout)
		}
	}
	os.Exit(1)
}
