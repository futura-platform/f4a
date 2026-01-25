package reliablewatch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/futura/privateencoding"
)

type ValueGetter[T any] func(tx fdb.ReadTransaction, key fdb.KeyConvertible, lastValue T) (T, error)

func PrivateEncodingGetter[T any](tr fdb.ReadTransaction, key fdb.KeyConvertible, _ T) (T, error) {
	currentValue, err := tr.Get(key).Get()
	if err != nil {
		var z T
		return z, err
	}
	// If the key doesn't exist, return the zero value
	if currentValue == nil {
		var z T
		return z, nil
	}
	decoder := privateencoding.NewDecoder[T](bytes.NewReader(currentValue))
	return decoder.Decode()
}

// Watch watches the key for changes and returns a channel of values and an error channel.
// On error, the error will be sent over the error channel and the watch loop will stop.
// This function is gauranteed to eventually commit getValue and send the returned T over the channel while the key is in its latest state,
// assuming no errors are encountered.
// It IS NOT gauranteed to receive every change that happens to the key.
// This function assumes that the value of this key is encoded using privateencoding.
func WatchCh[T any](
	ctx context.Context,
	db fdb.Transactor,
	initialKey fdb.KeyConvertible,
	// this will be the first "lastValue" passed to getValue
	initialValue T,
	// if this is not nil, watch will wait for this future to be committed before starting the watch loop and sending any values.
	initialWatch fdb.FutureNil,
	getValue ValueGetter[T],
) (<-chan T, <-chan error) {
	ch := make(chan T)
	errCh := make(chan error)
	startWatchLoop := func() error {
		handleWatchErr := func(err error) error {
			if ctx.Err() != nil {
				return fmt.Errorf("%w: %w", err, ctx.Err())
			}
			return fmt.Errorf("failed to get watch: %w", err)
		}

		var watch fdb.FutureNil = initialWatch
		var watchMu sync.Mutex
		go func() {
			<-ctx.Done()
			watchMu.Lock()
			if watch != nil {
				watch.Cancel()
			}
			watchMu.Unlock()
		}()
		if initialWatch != nil {
			err := initialWatch.Get()
			if err != nil {
				return handleWatchErr(err)
			}
		}

		var lastValue T = initialValue
		for ctx.Err() == nil {
			var v T
			_, err := db.Transact(func(tr fdb.Transaction) (any, error) {
				watchMu.Lock()
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				watch = tr.Watch(initialKey)
				watchMu.Unlock()
				var err error
				v, err = getValue(tr, initialKey, lastValue)
				return v, err
			})
			if err != nil {
				return fmt.Errorf("failed to get value: %w", err)
			}
			ch <- v
			lastValue = v

			err = watch.Get()
			if err != nil {
				return handleWatchErr(err)
			}
		}
		return ctx.Err()
	}
	go func() {
		defer close(ch)
		defer close(errCh)

		err := startWatchLoop()
		if err != nil {
			errCh <- err
		}
	}()
	return ch, errCh
}

var ErrOnChangeFailed = errors.New("on change failed")

// Watch watches the key for changes and returns an onChange function that can be used to register a callback for changes.
// The onChange function will block until an error is encountered in the listener, returning the error.
// getValue will be called to get the value whenever a change is detected on the given key.
func Watch[T any](
	ctx context.Context,
	db fdb.Transactor,
	key fdb.KeyConvertible,
	initialValue T,
	initialWatch fdb.FutureNil,
	getValue ValueGetter[T],
) (onChange func(func(T) error) error) {
	wctx, cancel := context.WithCancel(ctx)

	ch, errCh := WatchCh(wctx, db, key, initialValue, initialWatch, getValue)
	var mu sync.Mutex
	var onChangeErr error
	return func(onChange func(T) error) error {
		defer cancel()
		for {
			select {
			case value := <-ch:
				go func() {
					err := onChange(value)
					if err != nil {
						mu.Lock()
						onChangeErr = err
						mu.Unlock()
						cancel()
					}
				}()
			case err := <-errCh:
				a := ctx
				a.Err()
				mu.Lock()
				if onChangeErr != nil {
					return onChangeErr
				}
				mu.Unlock()
				return err
			}
		}
	}
}
