package api

import (
	"context"
	"errors"

	"connectrpc.com/connect"
	taskv1 "github.com/futura-platform/f4a/internal/gen/task/v1"
	"github.com/futura-platform/f4a/internal/gen/task/v1/taskv1connect"
	"github.com/futura-platform/f4a/internal/reliablequeue"
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
	deadLetterQueue reliablequeue.TFIFO[task.Id]
}

// PullDeadLetters implements taskv1connect.QueryServiceHandler.
func (q *queryService) PullDeadLetters(
	ctx context.Context,
	req *taskv1.PullDeadLettersRequest,
) (*taskv1.PullDeadLettersResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("PullDeadLetters is not implemented"))
}

// AcknowledgeDeadLetters implements taskv1connect.QueryServiceHandler.
func (q *queryService) AcknowledgeDeadLetters(
	ctx context.Context,
	req *taskv1.AcknowledgeDeadLettersRequest,
) (*taskv1.AcknowledgeDeadLettersResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("AcknowledgeDeadLetters is not implemented"))
}
