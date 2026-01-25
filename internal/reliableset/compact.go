package reliableset

import (
	"fmt"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

const (
	compactionInterval = 30 * time.Second
)

func (s *Set) runCompactionLoop() error {
	ticker := time.NewTicker(compactionInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.compactionContext.Done():
			return nil
		case <-ticker.C:
			_, err := s.t.Transact(func(tx fdb.Transaction) (any, error) {
				return nil, s.compactLog(tx)
			})
			if err != nil {
				// TODO: handle this logging more gracefully
				fmt.Printf("failed to compact log: %v\n", err)
				continue
			}
		}
	}
}

// compactLog compacts the current log into the snapshot.
func (s *Set) compactLog(tx fdb.Transaction) error {
	begin, end := s.logSubspace.FDBRangeKeys()
	logEntries, err := s.readLog(tx, begin)
	if err != nil {
		return err
	}
	for _, logEntry := range logEntries {
		logOperation := logEntry.entry.op
		value := logEntry.entry.value
		switch logOperation {
		case LogOperationAdd:
			tx.Set(s.snapshotSubspace.Pack(tuple.Tuple{value}), value)
		case LogOperationRemove:
			tx.Clear(s.snapshotSubspace.Pack(tuple.Tuple{value}))
		default:
			return fmt.Errorf("invalid log operation: %d", logOperation)
		}
	}
	tx.ClearRange(fdb.KeyRange{Begin: begin, End: end})
	return nil
}
