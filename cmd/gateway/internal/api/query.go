package api

import (
	"context"
	"fmt"

	"connectrpc.com/connect"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	taskv1 "github.com/futura-platform/f4a/internal/gen/task/v1"
	"github.com/futura-platform/f4a/internal/gen/task/v1/taskv1connect"
	"github.com/futura-platform/f4a/internal/servicestate"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
)

func NewQueryService(db dbutil.DbRoot) (taskv1connect.QueryServiceHandler, error) {
	deadLetterQueue, err := servicestate.CreateOrOpenDeadLetterQueue(db)
	if err != nil {
		return nil, err
	}

	return &queryService{
		db:              db,
		deadLetterQueue: deadLetterQueue,
	}, nil
}

type queryService struct {
	db              dbutil.DbRoot
	deadLetterQueue servicestate.DeadLetterQueue
}

// PullDeadLetters implements taskv1connect.QueryServiceHandler.
func (q *queryService) PullDeadLetters(
	ctx context.Context,
	req *taskv1.PullDeadLettersRequest,
) (*taskv1.PullDeadLettersResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var deadLetters []*taskv1.DeadLetter
	_, err := q.db.ReadTransactContext(ctx, func(tx fdb.ReadTransaction) (any, error) {
		pulled, err := q.deadLetterQueue.Pull(tx, int(req.GetMaxResults()))
		if err != nil {
			return nil, err
		}
		deadLetters = pulled
		return nil, nil
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("pull dead letters: %w", err))
	}
	return taskv1.PullDeadLettersResponse_builder{
		DeadLetters: deadLetters,
	}.Build(), nil
}

// AcknowledgeDeadLetters implements taskv1connect.QueryServiceHandler.
func (q *queryService) AcknowledgeDeadLetters(
	ctx context.Context,
	req *taskv1.AcknowledgeDeadLettersRequest,
) (*taskv1.AcknowledgeDeadLettersResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	taskIds := make([]task.Id, len(req.GetTaskIds()))
	for i, id := range req.GetTaskIds() {
		taskIds[i] = task.Id(id)
	}

	_, err := q.db.TransactContext(ctx, func(tx fdb.Transaction) (any, error) {
		q.deadLetterQueue.Acknowledge(tx, taskIds)
		return nil, nil
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, fmt.Errorf("acknowledge dead letters: %w", err))
	}
	return &taskv1.AcknowledgeDeadLettersResponse{}, nil
}
