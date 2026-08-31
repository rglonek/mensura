package ingest

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// Checkpoint is the resume record for one followed file. Only AckedOffset
// ever moves the resume point: it advances when the store has accepted a
// batch containing those bytes, so a crash replays at most one batch.
type Checkpoint struct {
	Stream      string `json:"stream"`
	Path        string `json:"path"`
	Fingerprint string `json:"fingerprint"`
	// FingerprintBytes is how many bytes Fingerprint covers. Zero means a
	// checkpoint written before the fingerprint carried its own width.
	FingerprintBytes int    `json:"fingerprint_bytes,omitempty"`
	Offset           int64  `json:"offset"`
	AckedOffset      int64  `json:"acked_offset"`
	LastTSMs         int64  `json:"last_ts_ms"`
	SpecHash         string `json:"spec_hash"`
	UpdatedUnix      int64  `json:"updated_unix"`
}

// CheckpointStore persists checkpoints as one small JSON file per stream,
// written atomically.
type CheckpointStore struct {
	dir string
	mu  sync.Mutex
	// Log, when set, reports a checkpoint that could not be read.
	Log Logger
}

func NewCheckpointStore(dir string) (*CheckpointStore, error) {
	if dir == "" {
		return &CheckpointStore{}, nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &CheckpointStore{dir: dir}, nil
}

// StreamID is the stable identity of a followed path.
func StreamID(path string) string {
	sum := sha256.Sum256([]byte(path))
	return hex.EncodeToString(sum[:8])
}

func (c *CheckpointStore) file(stream string) string {
	return filepath.Join(c.dir, stream+".json")
}

// Load reads a checkpoint. A missing file is a normal first run; an
// unreadable one is not, and is reported rather than silently treated as
// "start from the beginning".
func (c *CheckpointStore) Load(stream string) (*Checkpoint, bool) {
	if c.dir == "" {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	path := c.file(stream)
	b, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) && c.Log != nil {
			c.Log.Printf("WARNING checkpoint %s is unreadable (%v); this stream will be re-read", path, err)
		}
		return nil, false
	}
	var cp Checkpoint
	if err := json.Unmarshal(b, &cp); err != nil {
		if c.Log != nil {
			c.Log.Printf("WARNING checkpoint %s is corrupt (%v); this stream will be re-read", path, err)
		}
		return nil, false
	}
	return &cp, true
}

// Save writes a checkpoint atomically and durably: the temporary file is
// fsynced before the rename and the directory afterwards, because a
// checkpoint that survives only in the page cache is of no use for the one
// event it exists for.
func (c *CheckpointStore) Save(cp *Checkpoint) error {
	if c.dir == "" {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	b, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return err
	}
	tmp := c.file(cp.Stream) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, c.file(cp.Stream)); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if d, derr := os.Open(c.dir); derr == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}
