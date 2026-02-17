package task

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/reliablelock"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	"github.com/google/uuid"
)

const (
	revisionGCInterval       = 1 * time.Minute
	revisionGCAcquireTimeout = 2 * time.Second
	revisionGCSweepTimeout   = 10 * time.Second
	revisionGCBatchSize      = 256

	revisionGCLockLeaseDuration = 20 * time.Second
)

// RunRevisionGCLoop runs the revision GC loop.
// It will sweep expired tombstones.
// This is a necessary compromise to prevent the revision store from growing indefinitely.
func RunRevisionGCLoop(ctx context.Context, db dbutil.DbRoot) {
	store, err := CreateOrOpenRevisionStore(db)
	if err != nil {
		slog.Error("failed to initialize revision store for gc", "error", err)
		return
	}

	hostname, _ := os.Hostname()
	holderID := fmt.Sprintf("%s-%s", hostname, uuid.NewString())
	lock := reliablelock.NewLock[string](
		db,
		store.GCLockKey(),
		holderID,
		reliablelock.WithLeaseDuration(revisionGCLockLeaseDuration),
		reliablelock.WithRefreshInterval(revisionGCLockLeaseDuration/2),
	)

	ticker := time.NewTicker(revisionGCInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweepCtx, cancelSweep := context.WithTimeout(ctx, revisionGCSweepTimeout)
			err := runRevisionGCSweep(sweepCtx, db, store, lock)
			cancelSweep()
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				slog.Warn("revision gc sweep failed", "error", err)
			}
		}
	}
}

func runRevisionGCSweep(
	ctx context.Context,
	db dbutil.DbRoot,
	store RevisionStore,
	lock *reliablelock.Lock[string],
) error {
	acquireCtx, cancelAcquire := context.WithTimeout(ctx, revisionGCAcquireTimeout)
	err := lock.Acquire(acquireCtx)
	cancelAcquire()
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			// Another instance is likely sweeping.
			return nil
		}
		return fmt.Errorf("failed to acquire revision gc lock: %w", err)
	}
	defer func() {
		releaseErr := lock.Release()
		if releaseErr != nil &&
			!errors.Is(releaseErr, reliablelock.ErrNotOwner) &&
			!errors.Is(releaseErr, reliablelock.ErrNotHeld) {
			slog.Warn("failed to release revision gc lock", "error", releaseErr)
		}
	}()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		processed := 0
		_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
			count, sweepErr := store.SweepExpiredTombstones(tx, time.Now(), revisionGCBatchSize)
			if sweepErr != nil {
				return nil, sweepErr
			}
			processed = count
			return nil, nil
		})
		if err != nil {
			return fmt.Errorf("failed to sweep expired tombstones: %w", err)
		}
		if processed < revisionGCBatchSize {
			return nil
		}
	}
}
