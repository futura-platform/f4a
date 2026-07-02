package pool

import (
	"context"
	"errors"
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/f4a/internal/servicestate"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	otelutil "github.com/futura-platform/f4a/internal/util/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// DrainTaskRunner marks the runner as inactive and moves all tasks from its task set
// back to the pending set so they can be re-assigned by the dispatch service.
// It is idempotent: tasks already moved off the runner or deleted are skipped.
func DrainTaskRunner(
	ctx context.Context,
	dbr dbutil.DbRoot,
	placer *servicestate.TaskPlacer,
	runnerId string,
	activeRunners ActiveRunners,
	taskSet *servicestate.RunnerSet,
	taskDir task.TasksDirectory,
) (err error) {
	ctx, span := tracer.Start(ctx, "drainTaskRunner",
		trace.WithAttributes(attribute.String("runner_id", runnerId)),
	)
	defer func() { otelutil.End(span, err) }()

	// immediately mark runner as inactive when draining the pod.
	_, err = dbr.TransactContext(ctx, func(tx fdb.Transaction) (any, error) {
		activeRunners.SetActive(tx, runnerId, false)
		return nil, nil
	})
	if err != nil {
		return fmt.Errorf("failed to mark runner as inactive: %w", err)
	}

	hangingTasks, _, err := taskSet.Items(ctx, dbr.Database)
	if err != nil {
		return fmt.Errorf("failed to get task set items: %w", err)
	}

	// do a best effort to drain the task set, using batching to avoid overloading the tx size limit.
	const drainBatchSize = 128
	for hangingTasks.Cardinality() > 0 {
		currentBatch := mapset.NewSet[task.Id]()
		for range drainBatchSize {
			taskID, ok := hangingTasks.Pop()
			if !ok {
				break
			}
			currentBatch.Add(taskID)
		}
		// Collected inside the transaction closure and reset on each attempt
		// so FDB retries don't produce duplicates. Marker spans are emitted
		// only after the transaction commits.
		var requeued []task.Id
		_, err = dbr.TransactContext(ctx, func(tx fdb.Transaction) (any, error) {
			requeued = requeued[:0]
			for taskID := range currentBatch.Iter() {
				tkey, err := taskDir.Open(tx, task.Id(taskID))
				if err != nil {
					if errors.Is(err, directory.ErrDirNotExists) {
						// Task already completed/deleted while draining; skip idempotently.
						continue
					}
					return nil, fmt.Errorf("failed to open task: %w", err)
				}

				assignmentState, err := task.ReadAssignmentState(tx, tkey)
				if err != nil {
					return nil, fmt.Errorf("failed to read task assignment state: %w", err)
				}
				if err := assignmentState.ValidateRunnerIdInvariant(); err != nil {
					return nil, fmt.Errorf("task assignment invariant violation: %w", err)
				}
				isRunningOnThisRunner, err := assignmentState.IsRunningOn(runnerId)
				if err != nil {
					return nil, err
				}
				if !isRunningOnThisRunner {
					// Task has been moved off this runner already. skip idempotently.
					continue
				}

				err = placer.PlaceTaskIn(tx, servicestate.PlacementLocationPending, tkey)
				if err != nil {
					return nil, fmt.Errorf("failed to place task in pending set: %w", err)
				}
				requeued = append(requeued, taskID)
			}
			return nil, nil
		})
		if err != nil {
			return fmt.Errorf("failed to requeue tasks: %w", err)
		}
		// Emit a marker span per requeued task so the full task lifecycle can
		// be queried by task_id across traces.
		// Per-item spans alongside a batch span are an OTel-sanctioned
		// pattern, mirroring messaging semconv "create" spans (one per
		// message in a batch publish):
		// https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/#batch-publishing-with-create-spans
		for _, taskID := range requeued {
			_, taskSpan := tracer.Start(ctx, "requeueTask",
				trace.WithAttributes(
					attribute.String("task_id", string(taskID)),
					attribute.String("runner_id", runnerId),
				),
			)
			taskSpan.End()
		}
		hangingTasks.RemoveAll(currentBatch.ToSlice()...)
	}

	_, err = dbr.TransactContext(ctx, func(tx fdb.Transaction) (any, error) {
		return nil, taskSet.Clear(tx)
	})
	if err != nil {
		return fmt.Errorf("failed to clear task set: %w", err)
	}
	return nil
}
