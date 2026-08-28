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
	Offset      int64  `json:"offset"`
	AckedOffset int64  `json:"acked_offset"`
	LastTSMs    int64  `json:"last_ts_ms"`
	SpecHash    string `json:"spec_hash"`
	UpdatedUnix int64  `json:"updated_unix"`
}

// CheckpointStore persists checkpoints as one small JSON file per stream,
// written atomically.
type CheckpointStore struct {
	dir string
	mu  sync.Mutex
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

func (c *CheckpointStore) Load(stream string) (*Checkpoint, bool) {
	if c.dir == "" {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	b, err := os.ReadFile(c.file(stream))
	if err != nil {
		return nil, false
	}
	var cp Checkpoint
	if json.Unmarshal(b, &cp) != nil {
		return nil, false
	}
	return &cp, true
}

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
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, c.file(cp.Stream))
}
