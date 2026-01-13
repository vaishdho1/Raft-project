package tester

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var ErrSnapshotOutdated = errors.New("snapshot ignored: index is stale")
var ErrFrozen = errors.New("Kill yourself for tests") //Used by tests to prevent zombie writes/reads

type Persister struct {
	mu        sync.Mutex
	diskmu    sync.Mutex //Used for frozen and rename
	raftstate []byte
	snapshot  []byte
	// Snapshot metadata. In disk mode, snapshots are always expected to be persisted
	// with metadata, and snapshot.bin is always expected to decode with the header format.
	snapMetaValid bool
	snapIndex     int
	snapTerm      int

	// Disk persistence config
	diskEnabled bool

	baseDir string
	peerTag string
	dir     string

	// If true, all disk writes are suppressed.
	writeFrozen bool

	// File paths
	raftPath  string // Path to raftstate.bin
	snapPath  string // Path to snapshot.bin
	sessionID int64  // This is used to make each start use this for temporary files to avoid collisions

	// Async snapshot writes (simple mode): monotonic token to avoid older async writes
	// overwriting newer ones when publishing snapshot.bin.
	asyncSnapToken    uint64
	lastSnapshotIndex int

	// Unique temp tokens for raftstate/snapshot writes. This avoids temp file collisions
	// when old and new instances overlap briefly during restart in the same process.
	raftTmpToken uint64
	snapTmpToken uint64
}

func MakePersister() *Persister {
	ps := &Persister{}
	ps.initDiskIfEnabled()
	return ps
}

func MakePersisterWithTag(tag string) *Persister {
	ps := &Persister{peerTag: tag}
	ps.initDiskIfEnabled()
	return ps
}
func cleanup(tag string) {
	if os.Getenv("PERSISTER_DISK") == "1" {
		base := os.Getenv("PERSISTER_DIR")
		if base == "" {
			base = "raft-data"
		}
		_ = os.RemoveAll(filepath.Join(base, tag))
	}

}

// MakePersisterWithTagFresh creates a brand-new persister for `tag`.
// In disk mode, it wipes any existing on-disk state for that tag so that tests
// start from a clean slate
func MakePersisterWithTagFresh(tag string, testName string) *Persister {

	compositeTag := tag
	if testName != "" {
		compositeTag = filepath.Join(testName, tag)
	}
	//Cleanup any previous directories with the composite tag
	cleanup(compositeTag)

	return MakePersisterWithTag(compositeTag)
}
func (ps *Persister) cleanUp() {
	ps.mu.Lock()
	disk := ps.diskEnabled
	dir := ps.dir
	base := ps.baseDir
	ps.mu.Unlock()
	if !disk {
		return
	}
	// Safety guards: never delete empty path or the base directory itself.
	if dir == "" || dir == "." || dir == string(filepath.Separator) {
		return
	}
	if base != "" && (dir == base || dir == filepath.Clean(base)) {
		return
	}
	_ = os.RemoveAll(dir)
}
func clone(orig []byte) []byte {
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

// This is used to freeze the disk writes during shutdown and cleanup
func (ps *Persister) freezePersister() {
	ps.mu.Lock()
	ps.writeFrozen = true
	ps.mu.Unlock()
}

func (ps *Persister) Copy() *Persister {

	// Freeze the old persister so late goroutines can't overwrite disk files.
	ps.mu.Lock()
	disk := ps.diskEnabled
	tag := ps.peerTag

	// Snapshot/raft bytes for in-memory mode.
	rs := clone(ps.raftstate)
	snap := clone(ps.snapshot)
	metaOK := ps.snapMetaValid
	si := ps.snapIndex
	st := ps.snapTerm
	//ps.writeFrozen = true
	ps.mu.Unlock()

	// Disk mode: model a real crash/restart by reloading state from disk.
	// This ensures restarts only see what was actually persisted (tmp+fsync+rename).
	if disk {
		return MakePersisterWithTag(tag)
	}

	// In in-memory mode, still preserve the peer tag so logs/debug output can
	// attribute raftstate/snapshot writes to the right server.
	np := MakePersisterWithTag(tag)
	np.raftstate = rs
	np.snapshot = snap
	np.snapMetaValid = metaOK
	np.snapIndex = si
	np.snapTerm = st
	return np
}

func (ps *Persister) ReadRaftState() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}

func (ps *Persister) RaftStateSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

func (ps *Persister) ReadSnapshot() []byte {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

// ReadSnapshotMeta returns the latest snapshot payload bytes along with metadata (index, term)
// if the persister has it (e.g., disk snapshot file includes header, or SaveSnapshotWithMeta
// was used). If ok==false, callers should treat the snapshot as "opaque bytes only".
func (ps *Persister) ReadSnapshotMeta() (snapshot []byte, lastIncludedIndex int, lastIncludedTerm int) {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if !ps.snapMetaValid {
		return clone(ps.snapshot), 0, 0
	}
	return clone(ps.snapshot), ps.snapIndex, ps.snapTerm
}

func (ps *Persister) SnapshotSize() int {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}

// Save mimics the lab's interface: save both state and snapshot synchronously.
// Used for simple tests or if you revert to synchronous mode.
func (ps *Persister) Save(raftstate []byte, snapshot []byte) {
	ps.mu.Lock()
	ps.raftstate = clone(raftstate)
	disk := ps.diskEnabled
	ps.diskmu.Lock()
	frozen := ps.writeFrozen
	ps.diskmu.Unlock()
	// IMPORTANT: Save() does not carry snapshot metadata (index,term). In our "always meta"
	// design, disk snapshots must be written via SaveSnapshotWithMeta/BeginSnapshotWriteAsyncWithMeta.
	// We still update in-memory snapshot bytes for compatibility with lab-style usage.
	ps.snapshot = clone(snapshot)
	ps.mu.Unlock()

	if disk {
		if frozen {
			return
		}
		// Always persist raftstate.
		_ = ps.writeAtomic(ps.raftPath, ps.nextTmpPath("raftstate"), nil, raftstate)

		// Only persist snapshot if metadata is already known. Otherwise, refuse to write a raw
		// snapshot file that can't be safely recovered with your crash scenario constraints.
		if len(snapshot) > 0 {
			ps.mu.Lock()
			ok := ps.snapMetaValid
			idx := ps.snapIndex
			term := ps.snapTerm
			dir := ps.dir
			ps.mu.Unlock()
			if !ok {
				panic("Persister.Save: disk snapshots require metadata; use SaveSnapshotWithMeta/BeginSnapshotWriteAsyncWithMeta")
			}
			encoded := encodeSnapshotWithMeta(snapshot, idx, term)
			_ = ps.writeAtomic(ps.snapPath, ps.nextTmpPath("snapshot"), nil, encoded)
			_ = syncDir(dir)
		}
	}
}

// -------------------------------------------------------------------------
// Disk Persistence Implementation
// -------------------------------------------------------------------------
// This is for the tests specifically
func (ps *Persister) initDiskIfEnabled() {
	if os.Getenv("PERSISTER_DISK") != "1" {
		return
	}
	ps.diskEnabled = true

	base := os.Getenv("PERSISTER_DIR")
	if base == "" {
		base = "raft-data" // Default directory
	}
	//Creates a directory for each peer if it doesnt exist
	ps.baseDir = base
	if ps.peerTag != "" {
		ps.dir = filepath.Join(base, ps.peerTag)
	} else {
		ps.dir = base
	}

	// Set paths
	ps.raftPath = filepath.Join(ps.dir, "raftstate.bin")
	//ps.raftTmpPath = filepath.Join(ps.dir, "raftstate.tmp")
	ps.snapPath = filepath.Join(ps.dir, "snapshot.bin")
	//ps.snapTmpPath = filepath.Join(ps.dir, "snapshot.tmp")
	//Create a uniqeu-id for each restart
	ps.sessionID = time.Now().UnixNano()
	//Decide whether its a fresh persister or not
	_ = os.MkdirAll(ps.dir, 0755)

	ps.mu.Lock()
	defer ps.mu.Unlock()

	// RECOVERY: Attempt to load from disk on startup
	// 1. Load Raft State
	if data, err := os.ReadFile(ps.raftPath); err == nil && len(data) > 0 {
		ps.raftstate = data
	}
	if data, err := os.ReadFile(ps.snapPath); err == nil && len(data) > 0 {
		// "Always meta" behavior: snapshot.bin must decode with metadata header.
		// If it doesn't, treat it as invalid (best-effort delete) and continue with no snapshot.
		if payload, idx, term, ok := decodeSnapshotWithMeta(data); ok {
			ps.snapshot = payload
			ps.snapMetaValid = true
			ps.snapIndex = idx
			ps.snapTerm = term
			//Store the last snapshotIndex.This will be used to serialize snapshots
			ps.lastSnapshotIndex = idx
		} else {
			ps.snapshot = nil
			ps.snapMetaValid = false
			ps.snapIndex = 0
			ps.snapTerm = 0
			_ = os.Remove(ps.snapPath)
		}
	}
}

// -------------------------------------------------------------------------
// Snapshot file format with metadata (simple mode, no manifest required)
// -------------------------------------------------------------------------

// encodeSnapshotWithMeta stores metadata in the snapshot file without changing what
// Raft/KV see via ReadSnapshot() (they should still see only the raw snapshot bytes).
//
// Layout (little endian):
//
//	magic u32 = "SNAP"
//	ver   u32 = 1
//	index i64
//	term  i64
//	n     u64 (length of payload)
//	payload bytes
const (
	snapMagic uint32 = 0x50414e53 // "SNAP" in little endian
	snapVer   uint32 = 1
)

func encodeSnapshotWithMeta(payload []byte, lastIncludedIndex int, lastIncludedTerm int) []byte {
	hdrLen := 4 + 4 + 8 + 8 + 8
	b := make([]byte, hdrLen+len(payload))
	binary.LittleEndian.PutUint32(b[0:4], snapMagic)
	binary.LittleEndian.PutUint32(b[4:8], snapVer)
	binary.LittleEndian.PutUint64(b[8:16], uint64(int64(lastIncludedIndex)))
	binary.LittleEndian.PutUint64(b[16:24], uint64(int64(lastIncludedTerm)))
	binary.LittleEndian.PutUint64(b[24:32], uint64(len(payload)))
	copy(b[32:], payload)
	return b
}

func decodeSnapshotWithMeta(fileBytes []byte) (payload []byte, lastIncludedIndex int, lastIncludedTerm int, ok bool) {
	if len(fileBytes) < 32 {
		return nil, 0, 0, false
	}
	if binary.LittleEndian.Uint32(fileBytes[0:4]) != snapMagic {
		return nil, 0, 0, false
	}
	if binary.LittleEndian.Uint32(fileBytes[4:8]) != snapVer {
		return nil, 0, 0, false
	}
	idx := int(int64(binary.LittleEndian.Uint64(fileBytes[8:16])))
	term := int(int64(binary.LittleEndian.Uint64(fileBytes[16:24])))
	n := int(binary.LittleEndian.Uint64(fileBytes[24:32]))
	if n < 0 || 32+n != len(fileBytes) {
		return nil, 0, 0, false
	}
	pl := make([]byte, n)
	copy(pl, fileBytes[32:])
	return pl, idx, term, true
}

// SaveSnapshotWithMeta persists snapshot payload bytes while also storing (index,term) in the
// on-disk snapshot file header. This is synchronous (used for InstallSnapshot correctness).
func (ps *Persister) SaveSnapshotSync(snapshotPayload []byte, lastIncludedIndex int, lastIncludedTerm int) error {
	validator := func() error {
		if lastIncludedIndex <= ps.lastSnapshotIndex {
			return ErrSnapshotOutdated
		}
		ps.lastSnapshotIndex = lastIncludedIndex
		return nil
	}
	ps.mu.Lock()

	if ps.writeFrozen {
		ps.mu.Unlock()
		return ErrFrozen
	}
	//We already have a more recent snapshot so ignore this and return the error
	if lastIncludedIndex <= ps.lastSnapshotIndex {
		ps.mu.Unlock()
		return ErrSnapshotOutdated
	}
	//Check this:This is an actual write to the main snapshot file and should happen with the lock taken right
	ps.snapshot = clone(snapshotPayload)
	ps.snapMetaValid = true
	ps.snapIndex = lastIncludedIndex
	ps.snapTerm = lastIncludedTerm
	disk := ps.diskEnabled
	dir := ps.dir

	if !disk {
		ps.lastSnapshotIndex = lastIncludedIndex
		ps.mu.Unlock()
		return nil
	}
	ps.mu.Unlock()
	encoded := encodeSnapshotWithMeta(snapshotPayload, lastIncludedIndex, lastIncludedTerm)
	// Snapshot writes are rare; pay the extra directory fsync so snapshot.bin publish is durable.
	err := ps.writeAtomic(ps.snapPath, ps.nextTmpPath("snapshot"), validator, encoded)
	//Check if there was an error here
	if err != nil {
		return err
	}
	return syncDir(dir)
}

func (ps *Persister) SaveSnapshotAsync(snapshotPayload []byte, lastIncludedIndex int, lastIncludedTerm int) <-chan error {
	//To make sure we dont loose the reply
	done := make(chan error, 1)

	ps.mu.Lock()
	dir := ps.dir
	//frozen := ps.writeFrozen
	if ps.writeFrozen {
		ps.mu.Unlock()
		done <- ErrFrozen
		close(done)
		return done
	}
	// Reject stale snapshots BEFORE updating in-memory snapshot bytes.
	if lastIncludedIndex <= ps.lastSnapshotIndex {
		ps.mu.Unlock()
		done <- ErrSnapshotOutdated
		close(done)
		return done
	}

	ps.snapshot = clone(snapshotPayload)
	ps.snapMetaValid = true
	ps.snapIndex = lastIncludedIndex
	ps.snapTerm = lastIncludedTerm
	// IMPORTANT: lastSnapshotIndex must advance even in non-disk mode to reject stale snapshots.
	if !ps.diskEnabled {
		ps.lastSnapshotIndex = lastIncludedIndex
		close(done)
		ps.mu.Unlock()
		return done
	}
	ps.mu.Unlock()

	data := encodeSnapshotWithMeta(snapshotPayload, lastIncludedIndex, lastIncludedTerm)
	go func() {
		defer close(done)
		validator := func() error {
			if lastIncludedIndex <= ps.lastSnapshotIndex {
				return ErrSnapshotOutdated
			}
			ps.lastSnapshotIndex = lastIncludedIndex
			return nil
		}
		ps.mu.Lock()
		//latest := ps.asyncSnapToken == token
		frozenNow := ps.writeFrozen
		ps.mu.Unlock()
		if frozenNow {
			done <- ErrFrozen
			return
		}
		if err := ps.writeAtomic(ps.snapPath, ps.nextTmpPath("snapshot"), validator, data); err != nil {
			done <- err
			return
		}
		if err := syncDir(dir); err != nil {
			done <- err
			return
		}
	}()

	return done
}

// SaveRaftState writes only the Raft state.
// Use this for normal AppendEntries persistence.
func (ps *Persister) SaveRaftState(raftstate []byte) error {
	ps.mu.Lock()
	ps.raftstate = clone(raftstate)
	disk := ps.diskEnabled
	frozen := ps.writeFrozen
	ps.mu.Unlock()
	if !disk {
		return nil
	}

	if frozen {
		return ErrFrozen
	}
	err := ps.writeAtomic(ps.raftPath, ps.nextTmpPath("raftstate"), nil, raftstate)
	return err
}

// -------------------------------------------------------------------------
// Helper Functions
// -------------------------------------------------------------------------

// writeAtomic writes data to tmpPath, syncs, then renames to finalPath.
func (ps *Persister) writeAtomic(finalPath, tmpPath string, validator func() error, data []byte) error {

	if err := ps.writeAndSync(tmpPath, data); err != nil {
		return err
	}
	ps.mu.Lock()

	if ps.writeFrozen {
		ps.mu.Unlock()
		os.Remove(tmpPath)
		return ErrFrozen
	}
	//Run the check for validator: This is snapshot check for staleness
	if validator != nil {
		if err := validator(); err != nil {
			ps.mu.Unlock()
			os.Remove(tmpPath)
			return err
		}

	}
	//Rename to finalPath
	if err := os.Rename(tmpPath, finalPath); err != nil {
		ps.mu.Unlock()
		// In tests, configs can delete per peer directories between test cases while
		// async persistence goroutines from the previous test are still unwinding.
		// Treat missing tmp/dir as "aborted write".
		if os.IsNotExist(err) {
			_ = os.Remove(tmpPath)
			return ErrFrozen
		}
		_ = os.Remove(tmpPath)
		return err
	}
	ps.mu.Unlock()
	return nil
}

// writeAndSync writes data to path and calls Fsync.
func (ps *Persister) writeAndSync(path string, data []byte) error {
	ps.mu.Lock()
	frozen := ps.writeFrozen
	ps.mu.Unlock()
	if frozen {
		return nil
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		// In tests, per-peer directories can be wiped while old goroutines are still
		// unwinding. Treat missing dirs/files as "aborted write".
		if os.IsNotExist(err) {
			return ErrFrozen
		}
		return err
	}

	_, err = f.Write(data)
	if err != nil {
		f.Close()
		return err
	}

	// BARRIER: Force write to physical disk
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}

	return f.Close()
}

// nextTmpPath allocates a unique temp file path under ps.dir.
// It is safe to call without holding ps.mu.
func (ps *Persister) nextTmpPath(prefix string) string {
	ps.mu.Lock()
	defer ps.mu.Unlock()
	if prefix == "snapshot" {
		ps.snapTmpToken++
		return filepath.Join(ps.dir, fmt.Sprintf("%s-%d-%d.tmp", prefix, ps.sessionID, ps.snapTmpToken))
	}
	ps.raftTmpToken++
	return filepath.Join(ps.dir, fmt.Sprintf("%s-%d-%d.tmp", prefix, ps.sessionID, ps.raftTmpToken))
}

// This is used to sync directory in the end
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		// In tests, per-peer directories can be wiped while old goroutines are still
		// unwinding. Treat missing dirs as a benign "aborted write".
		if os.IsNotExist(err) {
			return ErrFrozen
		}
		return err
	}
	defer f.Close()
	return f.Sync()
}
