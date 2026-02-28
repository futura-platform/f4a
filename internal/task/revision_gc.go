package task

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/cenkalti/backoff/v4"
	"github.com/futura-platform/f4a/internal/reliablelock"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
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

	ticker := time.NewTicker(revisionGCInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweepCtx, cancelSweep := context.WithTimeout(ctx, revisionGCSweepTimeout)
			err := runRevisionGCSweep(sweepCtx, db, store)
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
) error {
	acquireCtx, cancelAcquire := context.WithTimeout(ctx, revisionGCAcquireTimeout)
	lease, err := store.GCLock().Acquire(acquireCtx, db.Database, reliablelock.DefaultLeaseOptions())
	cancelAcquire()
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			// Another instance is likely sweeping.
			return nil
		}
		return fmt.Errorf("failed to acquire revision gc lock: %w", err)
	}
	activeLease, err := lease.Activate(ctx)
	if err != nil {
		return fmt.Errorf("failed to activate revision gc lock: %w", err)
	}
	defer activeLease.BestEffortRelease(ctx, backoff.WithMaxElapsedTime(10*time.Second))
	ctx = activeLease

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
