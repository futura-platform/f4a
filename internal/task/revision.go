package task

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

const (
	revisionsDirectoryName  = "task_revisions"
	tombstonesDirectoryName = "task_revision_tombstones"

	revisionFieldLastApplied = "last_applied_revision"
	revisionFieldDeleted     = "deleted"
	revisionFieldGCLock      = "gc_lock"

	defaultTombstoneTTL = 24 * time.Hour
)

var (
	ErrInvalidRevision         = errors.New("revision must be >= 1")
	ErrCreateRevisionMustBeOne = errors.New("create task revision must be 1")
	ErrRevisionGap             = errors.New("revision gap")
)

type RevisionOperation byte

const (
	RevisionOperationCreate RevisionOperation = iota
	// RevisionOperationMutate covers operations that don't need special
	// revision-side metadata handling (update/activate/suspend).
	RevisionOperationMutate
	RevisionOperationDelete
)

type RevisionDecision byte

const (
	RevisionDecisionApplied RevisionDecision = iota
	RevisionDecisionDuplicate
)

type RevisionStore struct {
	revisions  directory.DirectorySubspace
	tombstones directory.DirectorySubspace
	gcLockKey  fdb.Key

	tombstoneTTL time.Duration
	now          func() time.Time
}

type tombstoneRecord struct {
	key      fdb.Key
	taskID   Id
	revision uint64
}

func CreateOrOpenRevisionStore(db dbutil.DbRoot) (RevisionStore, error) {
	revisions, err := db.Root.CreateOrOpen(db, []string{revisionsDirectoryName}, nil)
	if err != nil {
		return RevisionStore{}, fmt.Errorf("failed to create or open revisions directory: %w", err)
	}
	tombstones, err := db.Root.CreateOrOpen(db, []string{tombstonesDirectoryName}, nil)
	if err != nil {
		return RevisionStore{}, fmt.Errorf("failed to create or open tombstones directory: %w", err)
	}
	return RevisionStore{
		revisions:    revisions,
		tombstones:   tombstones,
		gcLockKey:    revisions.Pack(tuple.Tuple{revisionFieldGCLock}),
		tombstoneTTL: defaultTombstoneTTL,
		now:          time.Now,
	}, nil
}

func (s RevisionStore) GCLockKey() fdb.Key {
	return append(fdb.Key(nil), s.gcLockKey...)
}

func (s RevisionStore) Apply(
	tx fdb.Transaction,
	id Id,
	revision uint64,
	operation RevisionOperation,
	apply func() error,
) (RevisionDecision, error) {
	if revision == 0 {
		return 0, ErrInvalidRevision
	}

	switch operation {
	case RevisionOperationCreate, RevisionOperationMutate, RevisionOperationDelete:
	default:
		return 0, fmt.Errorf("unknown revision operation: %d", operation)
	}

	if operation == RevisionOperationCreate && revision != 1 {
		return 0, fmt.Errorf("%w: got %d", ErrCreateRevisionMustBeOne, revision)
	}

	currentRevision, err := s.lastAppliedRevision(tx, id)
	if err != nil {
		return 0, fmt.Errorf("failed to read current revision: %w", err)
	}
	if revision <= currentRevision {
		return RevisionDecisionDuplicate, nil
	}

	expectedRevision := currentRevision + 1
	if revision != expectedRevision {
		return 0, fmt.Errorf("%w: expected %d, got %d", ErrRevisionGap, expectedRevision, revision)
	}

	if err := apply(); err != nil {
		return 0, err
	}

	switch operation {
	case RevisionOperationCreate:
		tx.Set(s.deletedKey(id), encodeBool(false))
	case RevisionOperationMutate:
		// No special metadata transitions for middle-state operations.
	case RevisionOperationDelete:
		tx.Set(s.deletedKey(id), encodeBool(true))
		tx.Set(s.tombstoneKey(id, revision), s.tombstoneValue(id, revision))
	default:
		return 0, fmt.Errorf("unknown revision operation: %d", operation)
	}
	tx.Set(s.lastAppliedRevisionKey(id), encodeUint64(revision))

	return RevisionDecisionApplied, nil
}

func (s RevisionStore) SweepExpiredTombstones(tx fdb.Transaction, now time.Time, limit int) (int, error) {
	records, err := s.expiredTombstones(tx, now, limit)
	if err != nil {
		return 0, err
	}

	for _, record := range records {
		currentRevision, err := s.lastAppliedRevision(tx, record.taskID)
		if err != nil {
			return 0, fmt.Errorf("failed to read revision for %s: %w", record.taskID, err)
		}
		deleted, err := s.deleted(tx, record.taskID)
		if err != nil {
			return 0, fmt.Errorf("failed to read deleted marker for %s: %w", record.taskID, err)
		}

		if deleted && currentRevision == record.revision {
			tx.Clear(s.lastAppliedRevisionKey(record.taskID))
			tx.Clear(s.deletedKey(record.taskID))
		}
		tx.Clear(record.key)
	}

	return len(records), nil
}

func (s RevisionStore) lastAppliedRevisionKey(id Id) fdb.Key {
	return s.revisions.Pack(tuple.Tuple{string(id), revisionFieldLastApplied})
}

func (s RevisionStore) deletedKey(id Id) fdb.Key {
	return s.revisions.Pack(tuple.Tuple{string(id), revisionFieldDeleted})
}

func (s RevisionStore) tombstoneKey(id Id, revision uint64) fdb.Key {
	expiresAt := s.now().Add(s.tombstoneTTL).UnixNano()
	return s.tombstones.Pack(tuple.Tuple{expiresAt, string(id), revision})
}

func (s RevisionStore) tombstoneValue(id Id, revision uint64) []byte {
	return tuple.Tuple{string(id), revision}.Pack()
}

func (s RevisionStore) lastAppliedRevision(tx fdb.ReadTransaction, id Id) (uint64, error) {
	raw, err := tx.Get(s.lastAppliedRevisionKey(id)).Get()
	if err != nil {
		return 0, err
	}
	return decodeUint64(raw)
}

func (s RevisionStore) deleted(tx fdb.ReadTransaction, id Id) (bool, error) {
	raw, err := tx.Get(s.deletedKey(id)).Get()
	if err != nil {
		return false, err
	}
	return decodeBool(raw)
}

func (s RevisionStore) expiredTombstones(tx fdb.ReadTransaction, now time.Time, limit int) ([]tombstoneRecord, error) {
	if limit <= 0 {
		limit = 256
	}

	begin, _ := s.tombstones.FDBRangeKeys()
	end := s.tombstones.Pack(tuple.Tuple{now.UnixNano() + 1})
	kvs, err := tx.GetRange(
		fdb.KeyRange{Begin: begin, End: end},
		fdb.RangeOptions{Limit: limit},
	).GetSliceWithError()
	if err != nil {
		return nil, fmt.Errorf("failed to read expired tombstones: %w", err)
	}

	records := make([]tombstoneRecord, 0, len(kvs))
	for _, kv := range kvs {
		unpacked, err := s.tombstones.Unpack(kv.Key)
		if err != nil {
			return nil, fmt.Errorf("failed to unpack tombstone key: %w", err)
		}
		if len(unpacked) != 3 {
			return nil, fmt.Errorf("invalid tombstone key shape: %v", unpacked)
		}

		taskID, ok := unpacked[1].(string)
		if !ok {
			return nil, fmt.Errorf("invalid tombstone key task id type: %T", unpacked[1])
		}
		revision, err := tupleUint64(unpacked[2])
		if err != nil {
			return nil, fmt.Errorf("invalid tombstone key revision: %w", err)
		}

		valueTaskID, valueRevision, err := decodeTombstoneValue(kv.Value)
		if err != nil {
			return nil, fmt.Errorf("failed to decode tombstone value: %w", err)
		}
		if valueTaskID != Id(taskID) || valueRevision != revision {
			return nil, fmt.Errorf(
				"tombstone key/value mismatch: key=(%s,%d) value=(%s,%d)",
				taskID, revision, valueTaskID, valueRevision,
			)
		}

		records = append(records, tombstoneRecord{
			key:      append(fdb.Key(nil), kv.Key...),
			taskID:   Id(taskID),
			revision: revision,
		})
	}

	return records, nil
}

func encodeUint64(v uint64) []byte {
	var out [8]byte
	// NOTE: little endian matches existing atomic increment usage.
	binary.LittleEndian.PutUint64(out[:], v)
	return out[:]
}

func decodeUint64(raw []byte) (uint64, error) {
	if raw == nil {
		return 0, nil
	}
	if len(raw) != 8 {
		return 0, fmt.Errorf("expected 8 bytes for uint64, got %d", len(raw))
	}
	return binary.LittleEndian.Uint64(raw), nil
}

func encodeBool(v bool) []byte {
	if v {
		return []byte{1}
	}
	return []byte{0}
}

func decodeBool(raw []byte) (bool, error) {
	if raw == nil {
		return false, nil
	}
	if len(raw) != 1 {
		return false, fmt.Errorf("expected 1 byte for bool, got %d", len(raw))
	}
	switch raw[0] {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, fmt.Errorf("invalid bool byte: %d", raw[0])
	}
}

func decodeTombstoneValue(raw []byte) (Id, uint64, error) {
	unpacked, err := tuple.Unpack(raw)
	if err != nil {
		return "", 0, err
	}
	if len(unpacked) != 2 {
		return "", 0, fmt.Errorf("invalid tombstone value shape: %v", unpacked)
	}
	taskID, ok := unpacked[0].(string)
	if !ok {
		return "", 0, fmt.Errorf("invalid tombstone value task id type: %T", unpacked[0])
	}
	revision, err := tupleUint64(unpacked[1])
	if err != nil {
		return "", 0, err
	}
	return Id(taskID), revision, nil
}

func tupleUint64(v any) (uint64, error) {
	switch vv := v.(type) {
	case uint64:
		return vv, nil
	case int64:
		if vv < 0 {
			return 0, fmt.Errorf("negative integer %d cannot be uint64", vv)
		}
		return uint64(vv), nil
	default:
		return 0, fmt.Errorf("expected tuple integer (uint64/int64), got %T", v)
	}
}
