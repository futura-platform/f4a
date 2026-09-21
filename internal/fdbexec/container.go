package fdbexec

import (
	"context"
	"fmt"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/samber/mo"
)

// ExecutionContainer is a task's execution state, durably persisted in
// FoundationDB and served from memory.
type ExecutionContainer struct {
	db             fdb.Database
	tkey           task.TaskKey
	memoTable      subspace.Subspace
	callOrder      subspace.Subspace
	durableObjects subspace.Subspace

	// mu serializes write transactions with each other and with read
	// transactions, which run concurrently with one another.
	mu sync.RWMutex
	// state is the in-memory image: none until it is loaded. Protected by mu.
	state mo.Option[*executiontype.InMemoryContainer]
}

var _ executiontype.TransactionalContainer = &ExecutionContainer{}

// OpenTaskContainer opens a task container for the given task id and namespace.
// the namespace is used to isolate different sub execution containers, all scoped within the same task.
func OpenTaskContainer(
	db dbutil.DbRoot,
	tkey task.TaskKey,
	namespace string,
) *ExecutionContainer {
	return &ExecutionContainer{
		db:             db.Database,
		tkey:           tkey,
		memoTable:      tkey.MemoTable(namespace),
		callOrder:      tkey.CallOrder(namespace),
		durableObjects: tkey.DurableObjectSpace(namespace),
	}
}

// inMemory returns the in-memory container, loading it from the database if
// there is none. c.mu must be held for writing.
func (c *ExecutionContainer) inMemory(ctx context.Context) (*executiontype.InMemoryContainer, error) {
	if s, ok := c.state.Get(); ok {
		return s, nil
	}
	s, err := c.load(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load execution state: %w", err)
	}
	c.state = mo.Some(s)
	return s, nil
}

// Transact runs fn in a database transaction, then again over the in memory image.
func (c *ExecutionContainer) Transact(ctx context.Context, fn func(ctx context.Context, tx executiontype.Container) error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, err := c.inMemory(ctx)
	if err != nil {
		return err
	}

	_, err = c.db.Transact(func(t fdb.Transaction) (any, error) {
		if err := c.tkey.MustExist(t); err != nil {
			return nil, err
		}
		return nil, fn(ctx, &executionTransaction{
			Transaction: t,
			executionReadTransaction: executionReadTransaction{
				ReadTransaction: t,
				memoTable:       c.memoTable,
				callOrder:       c.callOrder,
				durableObjects:  c.durableObjects,
			},
		})
	})
	if err != nil {
		return err
	}
	return fn(ctx, s)
}

// ReadTransact runs fn over the image, with no database call (except for populating the in memory image if necessary).
func (c *ExecutionContainer) ReadTransact(ctx context.Context, fn func(ctx context.Context, tx executiontype.ReadOnlyContainer) error) error {
	c.mu.RLock()
	if s, ok := c.state.Get(); ok {
		defer c.mu.RUnlock()
		return fn(ctx, s)
	}
	c.mu.RUnlock()

	// not in memory: a read lock cannot be upgraded, so load under the write
	// lock and serve this one read from under it too
	c.mu.Lock()
	defer c.mu.Unlock()
	s, err := c.inMemory(ctx)
	if err != nil {
		return err
	}
	return fn(ctx, s)
}
