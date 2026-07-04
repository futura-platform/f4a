package servicestate

import (
	"context"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	taskv1 "github.com/futura-platform/f4a/internal/gen/task/v1"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

const deadLetterQueueDirectoryName = "task_dead_letter_queue"

type DeadLetterQueue struct {
	taskIdSerializer
	itemSpace subspace.Subspace
}

func (q DeadLetterQueue) itemKey(taskId task.Id) fdb.Key {
	return q.itemSpace.Pack(tuple.Tuple{q.Marshal(taskId)})
}

func (q DeadLetterQueue) taskIdFromKey(key fdb.Key) (task.Id, error) {
	unpacked, err := q.itemSpace.Unpack(key)
	if err != nil {
		return "", fmt.Errorf("unpack dead letter key: %w", err)
	} else if len(unpacked) != 1 {
		return "", fmt.Errorf("unexpected dead letter key shape: %d elements", len(unpacked))
	}
	raw, ok := unpacked[0].([]byte)
	if !ok {
		return "", fmt.Errorf("unexpected dead letter key element type %T", unpacked[0])
	}
	return q.Unmarshal(raw)
}

func CreateOrOpenDeadLetterQueue(db dbutil.DbRoot) (DeadLetterQueue, error) {
	itemSpace, err := db.Root.CreateOrOpen(db, []string{deadLetterQueueDirectoryName}, nil)
	if err != nil {
		return DeadLetterQueue{}, err
	}
	return DeadLetterQueue{
		itemSpace: itemSpace,
	}, nil
}

func (q DeadLetterQueue) Pull(tx fdb.ReadTransaction, maxResults int) ([]*taskv1.DeadLetter, error) {
	begin, end := q.itemSpace.FDBRangeKeys()
	items, err := tx.GetRange(
		fdb.KeyRange{Begin: begin, End: end},
		fdb.RangeOptions{Limit: maxResults},
	).GetSliceWithError()
	if err != nil {
		return nil, err
	}
	deadLetters := make([]*taskv1.DeadLetter, 0, len(items))
	for _, item := range items {
		taskId, err := q.taskIdFromKey(item.Key)
		if err != nil {
			return nil, err
		}
		result := &taskv1.TaskResult{}
		if err := proto.Unmarshal(item.Value, result); err != nil {
			return nil, fmt.Errorf("unmarshal dead letter result for %q: %w", taskId, err)
		}
		deadLetters = append(deadLetters, taskv1.DeadLetter_builder{
			TaskId: proto.String(string(taskId)),
			Result: result,
		}.Build())
	}
	return deadLetters, nil
}

func (q DeadLetterQueue) Acknowledge(tx fdb.Transaction, taskIds []task.Id) {
	for _, taskId := range taskIds {
		tx.Clear(q.itemKey(taskId))
	}
}

func (q DeadLetterQueue) Park(tx fdb.Transaction, taskId task.Id, result *taskv1.TaskResult) error {
	data, err := proto.Marshal(result)
	if err != nil {
		return fmt.Errorf("marshal task result: %w", err)
	}
	tx.Set(q.itemKey(taskId), data)
	return nil
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

func (p DeadLetterParker) Park(ctx context.Context, result *taskv1.TaskResult) error {
	// Parking is a rare failure path, so opening the queue lazily per park
	// keeps construction infallible without a meaningful cost.
	queue, err := CreateOrOpenDeadLetterQueue(p.db)
	if err != nil {
		return fmt.Errorf("failed to open dead letter queue: %w", err)
	}
	_, err = p.db.TransactContext(ctx, func(tx fdb.Transaction) (any, error) {
		return nil, queue.Park(tx, p.id, result)
	})
	if err != nil {
		return fmt.Errorf("failed to park dead letter: %w", err)
	}
	trace.SpanFromContext(ctx).AddEvent("task result parked on dead letter queue",
		trace.WithAttributes(attribute.String("task_id", string(p.id))),
	)
	return nil
}
