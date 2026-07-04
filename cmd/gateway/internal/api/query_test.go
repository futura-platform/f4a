package api

import (
	"context"
	"testing"

	"connectrpc.com/connect"
	"connectrpc.com/validate"
	taskv1 "github.com/futura-platform/f4a/internal/gen/task/v1"
	"github.com/futura-platform/f4a/internal/gen/task/v1/taskv1connect"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func newTestQueryClient(t *testing.T, db dbutil.DbRoot) taskv1connect.QueryServiceClient {
	t.Helper()

	handler, err := NewQueryService(db)
	require.NoError(t, err)

	_, queryHandler := taskv1connect.NewQueryServiceHandler(
		handler,
		connect.WithInterceptors(validate.NewInterceptor()),
	)
	server := testutil.NewEphemeralHTTPServer(t, queryHandler.ServeHTTP)
	return taskv1connect.NewQueryServiceClient(server.Client(), server.URL)
}

func TestQueryServicePullDeadLettersValidation(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		client := newTestQueryClient(t, db)

		t.Run("rejects zero max_results", func(t *testing.T) {
			_, err := client.PullDeadLetters(context.Background(), taskv1.PullDeadLettersRequest_builder{
				MaxResults: proto.Uint32(0),
			}.Build())
			require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
			require.Contains(t, err.Error(), "max_results")
		})

		t.Run("rejects max_results above limit", func(t *testing.T) {
			_, err := client.PullDeadLetters(context.Background(), taskv1.PullDeadLettersRequest_builder{
				MaxResults: proto.Uint32(1001),
			}.Build())
			require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
			require.Contains(t, err.Error(), "max_results")
		})

		t.Run("accepts max_results within limit", func(t *testing.T) {
			_, err := client.PullDeadLetters(context.Background(), taskv1.PullDeadLettersRequest_builder{
				MaxResults: proto.Uint32(100),
			}.Build())
			require.NoError(t, err)
		})

		t.Run("rejects unset max_results", func(t *testing.T) {
			_, err := client.PullDeadLetters(context.Background(), taskv1.PullDeadLettersRequest_builder{}.Build())
			require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
			require.Contains(t, err.Error(), "max_results")
		})
	})
}

func TestQueryServiceAcknowledgeDeadLettersValidation(t *testing.T) {
	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		client := newTestQueryClient(t, db)

		t.Run("rejects invalid task id", func(t *testing.T) {
			tooLong := string(make([]byte, 65))
			_, err := client.AcknowledgeDeadLetters(context.Background(), taskv1.AcknowledgeDeadLettersRequest_builder{
				TaskIds: []string{tooLong},
			}.Build())
			require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
			require.Contains(t, err.Error(), "task_ids")
		})
	})
}
