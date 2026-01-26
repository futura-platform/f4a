package reliableset

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"os"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

// The cursor is used to communicate which log entries have yet to be processed by read consumers.
// This allows the compaction process to have this information,
// which prevents race conditions between the compaction process and the read consumers that can cause entries to be skipped.

const (
	cursorLeaseTTL     = 45 * time.Second
	cursorLeaseRefresh = 15 * time.Second
	cursorKeyTail      = "tail"
	cursorKeyLease     = "lease"
	cursorKeyHint      = "hint"
)

func newConsumerID() (string, string) {
	hostname, _ := os.Hostname()
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return hex.EncodeToString([]byte(hostname)), hostname
	}
	return hex.EncodeToString(buf), hostname
}

func (s *Set) cursorKey(id, kind string) fdb.Key {
	return s.cursorSubspace.Pack(tuple.Tuple{id, kind})
}

func (s *Set) registerCursor(tx fdb.Transaction, tail fdb.KeyConvertible) {
	if s.consumerHint != "" {
		tx.Set(s.cursorKey(s.consumerID, cursorKeyHint), []byte(s.consumerHint))
	}
	s.writeCursorTail(tx, tail)
	s.writeLease(tx, time.Now())
}

func (s *Set) writeCursorTail(tx fdb.Transaction, tail fdb.KeyConvertible) {
	if tail == nil {
		panic("cursor tail is nil")
	}
	tx.Set(s.cursorKey(s.consumerID, cursorKeyTail), tail.FDBKey())
}

func (s *Set) writeLease(tx fdb.Transaction, now time.Time) {
	tx.Set(s.cursorKey(s.consumerID, cursorKeyLease), encodeLease(now.Add(cursorLeaseTTL)))
}

func (s *Set) advanceCursor(ctx context.Context, tail fdb.KeyConvertible) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := s.t.Transact(func(tx fdb.Transaction) (any, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		s.writeCursorTail(tx, tail)
		s.writeLease(tx, time.Now())
		return nil, nil
	})
	return err
}

// leaseLoop refreshes the lease of the cursor periodically
// This is used to prevent the cursor from being garbage collected,
// which would happen if this consumer dies for any reason.
func (s *Set) leaseLoop(ctx context.Context) {
	if cursorLeaseRefresh <= 0 {
		return
	}
	ticker := time.NewTicker(cursorLeaseRefresh)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = s.t.Transact(func(tx fdb.Transaction) (any, error) {
				s.writeLease(tx, time.Now())
				return nil, nil
			})
		}
	}
}

func encodeLease(exp time.Time) []byte {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], uint64(exp.UnixNano()))
	return buf[:]
}

func decodeLease(raw []byte) (time.Time, bool) {
	if len(raw) < 8 {
		return time.Time{}, false
	}
	return time.Unix(0, int64(binary.LittleEndian.Uint64(raw))), true
}

type cursorState struct {
	tail  fdb.Key
	lease time.Time
}

type cursorIndex struct {
	tails    map[string]fdb.Key
	leases   map[string]time.Time
	keysByID map[string][]fdb.Key
}

func (s *Set) decodeCursorKey(key fdb.Key) (string, string, bool) {
	decoded, err := s.cursorSubspace.Unpack(key)
	if err != nil || len(decoded) != 2 {
		return "", "", false
	}
	id, ok1 := decoded[0].(string)
	kind, ok2 := decoded[1].(string)
	if !ok1 || !ok2 {
		return "", "", false
	}
	return id, kind, true
}

// cursorIndex gets a snapshot of all current cursor states
func (s *Set) cursorIndex(tx fdb.ReadTransaction) (cursorIndex, error) {
	begin, end := s.cursorSubspace.FDBRangeKeys()
	kvs, err := tx.GetRange(fdb.KeyRange{Begin: begin, End: end}, fdb.RangeOptions{}).GetSliceWithError()
	if err != nil {
		return cursorIndex{}, err
	}
	index := cursorIndex{
		tails:    map[string]fdb.Key{},
		leases:   map[string]time.Time{},
		keysByID: map[string][]fdb.Key{},
	}
	for _, kv := range kvs {
		id, kind, ok := s.decodeCursorKey(kv.Key)
		if !ok {
			continue
		}
		index.keysByID[id] = append(index.keysByID[id], kv.Key)
		switch kind {
		case cursorKeyTail:
			index.tails[id] = append([]byte(nil), kv.Value...)
		case cursorKeyLease:
			if lease, ok := decodeLease(kv.Value); ok {
				index.leases[id] = lease
			}
		}
	}
	return index, nil
}

func activeCursors(index cursorIndex, now time.Time) map[string]cursorState {
	active := map[string]cursorState{}
	for id, lease := range index.leases {
		if !lease.After(now) {
			continue
		}
		tail := index.tails[id]
		if len(tail) == 0 {
			continue
		}
		active[id] = cursorState{tail: tail, lease: lease}
	}
	return active
}

// minActiveTail finds the lexicographically smallest tail of all active cursors
func minActiveTail(active map[string]cursorState) (fdb.Key, bool) {
	var min fdb.Key
	for _, state := range active {
		if min == nil || bytes.Compare(state.tail, min) < 0 {
			min = state.tail
		}
	}
	if min == nil {
		return nil, false
	}
	return min, true
}

func cleanDeadCursors(tx fdb.Transaction, index cursorIndex, now time.Time) error {
	for id, keys := range index.keysByID {
		lease, ok := index.leases[id]
		if ok && lease.After(now) {
			continue
		}
		for _, key := range keys {
			tx.Clear(key)
		}
	}
	return nil
}
