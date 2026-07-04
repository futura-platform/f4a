package servicestate

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/reliablequeue"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

const deadLetterQueueDirectoryName = "task_dead_letter_queue"

func CreateOrOpenDeadLetterQueue(db dbutil.DbRoot) (reliablequeue.TFIFO[task.Id], error) {
	return reliablequeue.CreateOrOpenTFIFO(db, []string{deadLetterQueueDirectoryName}, taskIdSerializer{})
}

// DeadLetterParker parks a specific task on the dead letter queue when its
// result delivery budget is exhausted. It satisfies execute.DeadLetterParker.
type DeadLetterParker struct {
	db dbutil.DbRoot
	id task.Id
}

func NewDeadLetterParker(db dbutil.DbRoot, id task.Id) DeadLetterParker {
	return DeadLetterParker{db: db, id: id}
}

func (p DeadLetterParker) Park(ctx context.Context, deliveryFailure string) error {
	// Parking is a rare failure path, so opening the queue lazily per park
	// keeps construction infallible without a meaningful cost.
	queue, err := CreateOrOpenDeadLetterQueue(p.db)
	if err != nil {
		return fmt.Errorf("failed to open dead letter queue: %w", err)
	}
	_, err = p.db.TransactContext(ctx, func(tx fdb.Transaction) (any, error) {
		return nil, queue.Enqueue(tx, p.id)
	})
	if err != nil {
		return fmt.Errorf("failed to enqueue dead letter: %w", err)
	}
	// The queue stores only the task id; the failure reason is preserved in
	// logs until dead letters carry richer metadata.
	slog.LogAttrs(ctx, slog.LevelWarn, "task result parked on dead letter queue",
		slog.String("task_id", string(p.id)),
		slog.String("delivery_failure", deliveryFailure),
	)
	return nil
}
