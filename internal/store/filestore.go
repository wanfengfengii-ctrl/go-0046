package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/benzhi/auction-pacing-reservation-engine/internal/config"
	"github.com/benzhi/auction-pacing-reservation-engine/internal/domain"
)

const (
	checkpointFile = "checkpoint.json"
	checkpointTmp  = "checkpoint.json.tmp"
	oplogFile      = "oplog.bin"
	ckptMagic      = "APRE-CKPT-1"
	frameHashLen   = 32
)

// File is a durable, pure-Go embedded store. It writes an append-only operation
// log of hash-chained frames and periodically snapshots state to an atomic
// checkpoint file. On open it loads the latest valid checkpoint and replays the
// subsequent log, rebuilding identical state.
type File struct {
	baseStore
	dir string
	mu  sync.Mutex // shadows baseStore.mu; File uses its own lock discipline
	f   *os.File   // oplog handle, opened for append
}

// checkpoint is the on-disk checkpoint document.
type checkpoint struct {
	Magic         string          `json:"magic"`
	ConfigVersion int64           `json:"config_version"`
	LastSeq       int64           `json:"last_seq"`
	LastHash      [32]byte        `json:"last_hash"`
	SummaryHash   [32]byte        `json:"summary_hash"`
	State         json.RawMessage `json:"state"`
}

// OpenFile opens (or creates) a durable store at dir. If a checkpoint and/or
// log exist, they are loaded and replayed. The config version must match any
// existing checkpoint's version or startup fails.
func OpenFile(dir string, cfg *config.Config) (*File, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}
	f := &File{dir: dir}

	state, lastSeq, lastHash, err := loadCheckpoint(dir, cfg.Version)
	if err != nil {
		return nil, err
	}
	if state == nil {
		// No checkpoint: build fresh state from config.
		state = BuildState(cfg)
		lastSeq = 0
		lastHash = [32]byte{}
	}
	f.state = state
	f.state.LastSeq = lastSeq
	f.state.LastHash = lastHash

	// Replay the log on top of the checkpoint.
	logPath := filepath.Join(dir, oplogFile)
	lf, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open oplog: %w", err)
	}
	if err := f.replay(lf); err != nil {
		lf.Close()
		return nil, err
	}
	// Move the file offset to the end for appending.
	if _, err := lf.Seek(0, io.SeekEnd); err != nil {
		lf.Close()
		return nil, fmt.Errorf("seek oplog end: %w", err)
	}
	f.f = lf
	return f, nil
}

func loadCheckpoint(dir string, cfgVersion int64) (*domain.State, int64, [32]byte, error) {
	path := filepath.Join(dir, checkpointFile)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, 0, [32]byte{}, nil
		}
		return nil, 0, [32]byte{}, fmt.Errorf("read checkpoint: %w", err)
	}
	var ck checkpoint
	if err := json.Unmarshal(data, &ck); err != nil {
		return nil, 0, [32]byte{}, fmt.Errorf("checkpoint corrupt: %w", err)
	}
	if ck.Magic != ckptMagic {
		return nil, 0, [32]byte{}, fmt.Errorf("checkpoint: bad magic %q", ck.Magic)
	}
	if ck.ConfigVersion != cfgVersion {
		return nil, 0, [32]byte{}, fmt.Errorf(
			"checkpoint: config version mismatch (state=%d, config=%d): refusing to start",
			ck.ConfigVersion, cfgVersion)
	}
	state, err := domain.DecodeState(ck.State)
	if err != nil {
		return nil, 0, [32]byte{}, fmt.Errorf("checkpoint: decode state: %w", err)
	}
	if got := state.SummaryHash(); got != ck.SummaryHash {
		return nil, 0, [32]byte{}, fmt.Errorf("checkpoint: summary hash mismatch (state=%x, recorded=%x)",
			got, ck.SummaryHash)
	}
	state.LastSeq = ck.LastSeq
	state.LastHash = ck.LastHash
	return state, ck.LastSeq, ck.LastHash, nil
}

// replay reads the oplog and applies each valid frame. A torn final frame
// (incomplete read) is truncated; any other corruption or chain break fails
// startup.
func (f *File) replay(lf *os.File) error {
	if _, err := lf.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek oplog start: %w", err)
	}
	reader := lf
	var lastGood int64
	expectedSeq := f.state.LastSeq
	expectedPrev := f.state.LastHash
	for {
		frame, payload, hash, n, err := readFrame(reader)
		if err != nil {
			if errors.Is(err, errTornFrame) {
				// Truncate the torn tail to the last good frame boundary.
				if tErr := lf.Truncate(lastGood); tErr != nil {
					return fmt.Errorf("truncate torn oplog: %w", tErr)
				}
				return nil
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		// Validate frame hash.
		if !bytes.Equal(frame.hash[:], hash[:]) {
			return fmt.Errorf("oplog: frame at offset %d hash mismatch (corruption)", lastGood)
		}
		op, err := domain.DecodeOp(payload)
		if err != nil {
			return fmt.Errorf("oplog: decode frame at offset %d: %v", lastGood, err)
		}
		if op.Seq <= f.state.LastSeq {
			// Pre-checkpoint entry: already captured in checkpoint state. Skip
			// application but still validate the frame hash (done above). Keep it
			// in the in-memory log so OpLog remains complete after restart.
			f.log = append(f.log, op)
			lastGood = n
			continue
		}
		if op.Seq != expectedSeq+1 {
			return fmt.Errorf("oplog: out-of-sequence frame: got seq %d, expected %d",
				op.Seq, expectedSeq+1)
		}
		if op.PrevHash != expectedPrev {
			return fmt.Errorf("oplog: chain break at seq %d: prev hash mismatch", op.Seq)
		}
		if err := f.state.Apply(op); err != nil {
			return fmt.Errorf("oplog: apply seq %d: %w", op.Seq, err)
		}
		expectedSeq = op.Seq
		expectedPrev = frame.hash
		f.state.LastSeq = op.Seq
		f.state.LastHash = frame.hash
		// Keep an in-memory copy for the ops endpoint.
		f.log = append(f.log, op)
		lastGood = n
	}
}

// rawFrame holds a parsed frame's computed hash and the offset past it.
type rawFrame struct {
	hash [32]byte
}

var errTornFrame = errors.New("torn frame")

// readFrame reads one length-prefixed frame. It returns the computed hash, the
// payload, the stored hash, the byte offset past the frame, and an error. A
// clean EOF at a frame boundary returns io.EOF; a partial frame returns
// errTornFrame.
func readFrame(r io.Reader) (rawFrame, []byte, [32]byte, int64, error) {
	var lenBuf [4]byte
	n, err := io.ReadFull(r, lenBuf[:])
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			if n == 0 {
				return rawFrame{}, nil, [32]byte{}, 0, io.EOF
			}
			return rawFrame{}, nil, [32]byte{}, 0, errTornFrame
		}
		return rawFrame{}, nil, [32]byte{}, 0, err
	}
	payloadLen := binary.BigEndian.Uint32(lenBuf[:])
	if payloadLen > 16<<20 { // 16 MiB sanity cap
		return rawFrame{}, nil, [32]byte{}, 0, fmt.Errorf("oplog: frame too large (%d)", payloadLen)
	}
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return rawFrame{}, nil, [32]byte{}, 0, errTornFrame
	}
	var stored [32]byte
	if _, err := io.ReadFull(r, stored[:]); err != nil {
		return rawFrame{}, nil, [32]byte{}, 0, errTornFrame
	}
	computed := sha256.Sum256(payload)
	return rawFrame{hash: computed}, payload, stored, int64(4 + payloadLen + 32), nil
}

// appendFrame writes a frame to the oplog and fsyncs. The commit point is the
// successful fsync: once it returns, the operation is durable.
func (f *File) appendFrame(op *domain.Op) (rawFrame, error) {
	payload, err := domain.EncodeOp(op)
	if err != nil {
		return rawFrame{}, err
	}
	var buf bytes.Buffer
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload)))
	buf.Write(lenBuf[:])
	buf.Write(payload)
	hash := sha256.Sum256(payload)
	buf.Write(hash[:])
	if _, err := f.f.Write(buf.Bytes()); err != nil {
		return rawFrame{}, fmt.Errorf("write oplog: %w", err)
	}
	if err := f.f.Sync(); err != nil {
		return rawFrame{}, fmt.Errorf("sync oplog: %w", err)
	}
	return rawFrame{hash: hash}, nil
}

// Apply runs fn under the store lock, persists the op, and applies it.
func (f *File) Apply(ctx context.Context, fn ApplyFn) (int64, *domain.OpResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	action, err := fn(f.state)
	if err != nil {
		return 0, nil, err
	}
	if action == nil {
		return 0, nil, domain.NewError(domain.CodeInternal, "nil action")
	}
	if action.Commit == nil {
		return 0, action.Result, nil
	}
	// Pre-commit cancellation check: before the commit point, a canceled
	// context must not change any state.
	if err := ctx.Err(); err != nil {
		return 0, nil, mapCtxError(err)
	}
	op := action.Commit
	op.Seq = f.state.LastSeq + 1
	op.PrevHash = f.state.LastHash
	if op.Result != nil {
		op.Result.Seq = op.Seq
	}
	// Commit point: durable append + fsync.
	frame, err := f.appendFrame(op)
	if err != nil {
		return 0, nil, err
	}
	// Apply to materialized state. The op was validated by fn; Apply re-checks
	// invariants. If it failed here the log would contain an unapplied op, but
	// recovery replays it identically, so live and recovered state stay
	// consistent. A failure indicates a logic bug.
	if err := f.state.Apply(op); err != nil {
		return 0, nil, err
	}
	f.state.LastSeq = op.Seq
	f.state.LastHash = frame.hash
	f.log = append(f.log, op)
	return op.Seq, op.Result, nil
}

// Snapshot returns a deep copy of the current state.
func (f *File) Snapshot(ctx context.Context) (*domain.State, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.state.Clone(), nil
}

// GetReservation returns a copy of a reservation.
func (f *File) GetReservation(ctx context.Context, id string) (*domain.Reservation, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.state.Reservation(id)
	if r == nil {
		return nil, domain.NewError(domain.CodeUnknownReservation, "reservation "+id)
	}
	cp := *r
	return &cp, nil
}

// GetIdempotency returns a copy of an idempotency record.
func (f *File) GetIdempotency(ctx context.Context, key string) (*domain.IdemRecord, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := f.state.IdemRecord(key)
	if r == nil {
		return nil, false
	}
	cp := *r
	if r.Result != nil {
		rc := *r.Result
		cp.Result = &rc
	}
	return &cp, true
}

// LastSeq returns the last committed sequence number.
func (f *File) LastSeq() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.state.LastSeq
}

// OpLog returns committed ops after the given sequence.
func (f *File) OpLog(ctx context.Context, after int64, limit int) ([]*domain.Op, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.opLog(after, limit), nil
}

// Checkpoint writes an atomic checkpoint of the current state.
func (f *File) Checkpoint(ctx context.Context) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.checkpointLocked()
}

func (f *File) checkpointLocked() (int64, error) {
	stateBytes, err := domain.EncodeState(f.state)
	if err != nil {
		return 0, err
	}
	ck := checkpoint{
		Magic:         ckptMagic,
		ConfigVersion: f.state.ConfigVersion,
		LastSeq:       f.state.LastSeq,
		LastHash:      f.state.LastHash,
		SummaryHash:   f.state.SummaryHash(),
		State:         json.RawMessage(stateBytes),
	}
	data, err := json.MarshalIndent(ck, "", "  ")
	if err != nil {
		return 0, fmt.Errorf("marshal checkpoint: %w", err)
	}
	tmpPath := filepath.Join(f.dir, checkpointTmp)
	finalPath := filepath.Join(f.dir, checkpointFile)
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return 0, fmt.Errorf("write checkpoint tmp: %w", err)
	}
	tmpF, err := os.Open(tmpPath)
	if err != nil {
		return 0, fmt.Errorf("open checkpoint tmp: %w", err)
	}
	if err := tmpF.Sync(); err != nil {
		tmpF.Close()
		return 0, fmt.Errorf("sync checkpoint tmp: %w", err)
	}
	tmpF.Close()
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return 0, fmt.Errorf("rename checkpoint: %w", err)
	}
	if err := syncDir(f.dir); err != nil {
		return 0, fmt.Errorf("sync checkpoint dir: %w", err)
	}
	return f.state.LastSeq, nil
}

// syncDir fsyncs the directory holding the named path's directory, to make a
// rename durable. It is best-effort on platforms that do not support directory
// fsync.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Close closes the oplog file.
func (f *File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.f != nil {
		err := f.f.Close()
		f.f = nil
		return err
	}
	return nil
}

// CorruptLogByte flips a byte in the oplog file at the given offset, for
// testing corruption detection. The store must be closed before corrupting.
func (f *File) CorruptLogByte(offset int64) error {
	path := filepath.Join(f.dir, oplogFile)
	return corruptFileByte(path, offset)
}

// corruptFileByte flips a byte at offset in the file at path.
func corruptFileByte(path string, offset int64) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	var b [1]byte
	if _, err := f.ReadAt(b[:], offset); err != nil {
		return err
	}
	b[0] ^= 0xFF
	if _, err := f.WriteAt(b[:], offset); err != nil {
		return err
	}
	return f.Sync()
}
