package reliablewatch

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	"github.com/futura-platform/f4a/pkg/util"
	testutil "github.com/futura-platform/f4a/pkg/util/test"
	"github.com/futura-platform/futura/privateencoding"
	"github.com/stretchr/testify/assert"
)

type mockFutureNil struct {
	readyCh   chan struct{}
	cancelCh  chan struct{}
	startedCh chan struct{}

	mu          sync.Mutex
	err         error
	canceled    bool
	readyOnce   sync.Once
	cancelOnce  sync.Once
	startedOnce sync.Once
}

func newMockFutureNil() *mockFutureNil {
	return &mockFutureNil{
		readyCh:   make(chan struct{}),
		cancelCh:  make(chan struct{}),
		startedCh: make(chan struct{}),
	}
}

func (f *mockFutureNil) Started() <-chan struct{} {
	return f.startedCh
}

func (f *mockFutureNil) Resolve(err error) {
	f.mu.Lock()
	f.err = err
	f.mu.Unlock()
	f.readyOnce.Do(func() { close(f.readyCh) })
}

func (f *mockFutureNil) BlockUntilReady() {
	select {
	case <-f.readyCh:
	case <-f.cancelCh:
	}
}

func (f *mockFutureNil) IsReady() bool {
	select {
	case <-f.readyCh:
		return true
	case <-f.cancelCh:
		return true
	default:
		return false
	}
}

func (f *mockFutureNil) Cancel() {
	f.cancelOnce.Do(func() {
		f.mu.Lock()
		f.canceled = true
		f.mu.Unlock()
		close(f.cancelCh)
	})
}

func (f *mockFutureNil) Get() error {
	f.startedOnce.Do(func() { close(f.startedCh) })
	f.BlockUntilReady()
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.canceled && f.err == nil {
		return context.Canceled
	}
	return f.err
}

func (f *mockFutureNil) MustGet() {
	if err := f.Get(); err != nil {
		panic(err)
	}
}

func TestWatch(t *testing.T) {
	toMonitorKey := func(db util.DbRoot) fdb.Key {
		return db.Root.Pack(tuple.Tuple{"toMonitor"})
	}
	updateToMonitorToString := func(db util.DbRoot, value string) error {
		_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
			buf := bytes.NewBuffer(nil)
			encoder := privateencoding.NewEncoder[string](buf)
			assert.NoError(t, encoder.Encode(value))

			tx.Set(toMonitorKey(db), buf.Bytes())
			return nil, nil
		})
		return err
	}

	t.Run("immedietely sends the current value, even if there is no change", func(t *testing.T) {
		t.Run("without initial value", func(t *testing.T) {
			testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
				stateCh, errCh := WatchCh[string](t.Context(), db, toMonitorKey(db), "", nil, PrivateEncodingGetter[string])
				select {
				case state := <-stateCh:
					assert.Zero(t, state)
				case err := <-errCh:
					t.Fatalf("watch failed: %v", err)
				}
			})
		})
		t.Run("with initial value", func(t *testing.T) {
			testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
				const expectedValue = "expectedValue"
				err := updateToMonitorToString(db, expectedValue)
				assert.NoError(t, err)

				stateCh, errCh := WatchCh[string](t.Context(), db, toMonitorKey(db), "", nil, PrivateEncodingGetter[string])
				select {
				case state := <-stateCh:
					assert.Equal(t, expectedValue, state)
				case err := <-errCh:
					t.Fatalf("watch failed: %v", err)
				}
			})
		})
	})

	t.Run("initial watch gates emissions", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			err := updateToMonitorToString(db, "initialValue")
			assert.NoError(t, err)

			initialWatch := newMockFutureNil()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			stateCh, errCh := WatchCh[string](ctx, db, toMonitorKey(db), "", initialWatch, PrivateEncodingGetter[string])
			select {
			case <-initialWatch.Started():
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for initial watch to start")
			}

			err = updateToMonitorToString(db, "updatedValue")
			assert.NoError(t, err)

			select {
			case value, ok := <-stateCh:
				if ok {
					t.Fatalf("unexpected value before initial watch resolved: %q", value)
				}
				t.Fatal("watch closed before initial watch resolved")
			case err := <-errCh:
				t.Fatalf("watch failed before initial watch resolved: %v", err)
			case <-time.After(100 * time.Millisecond):
			}

			initialWatch.Resolve(nil)

			select {
			case value := <-stateCh:
				assert.Equal(t, "updatedValue", value)
			case err := <-errCh:
				t.Fatalf("watch failed after initial watch resolved: %v", err)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for initial watch to resolve")
			}
		})
	})

	t.Run("initial watch error is returned", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			initialWatch := newMockFutureNil()
			stateCh, errCh := WatchCh[string](t.Context(), db, toMonitorKey(db), "", initialWatch, PrivateEncodingGetter[string])
			expectedErr := fmt.Errorf("initial watch error")

			initialWatch.Resolve(expectedErr)

			select {
			case err := <-errCh:
				assert.ErrorIs(t, err, expectedErr)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for initial watch error")
			}

			select {
			case value, ok := <-stateCh:
				if ok {
					t.Fatalf("unexpected value after initial watch error: %q", value)
				}
			default:
			}
		})
	})

	t.Run("basic watch", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			expectedValue := "expectedValue"
			_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
				buf := bytes.NewBuffer(nil)
				encoder := privateencoding.NewEncoder[string](buf)
				assert.NoError(t, encoder.Encode(expectedValue))
				tx.Set(toMonitorKey(db), buf.Bytes())
				return nil, nil
			})
			if err != nil {
				t.Fatalf("failed to set value: %v", err)
			}

			stateCh, errCh := WatchCh[string](t.Context(), db, toMonitorKey(db), "", nil, PrivateEncodingGetter[string])
			select {
			case state := <-stateCh:
				assert.Equal(t, expectedValue, state)
			case err := <-errCh:
				t.Fatalf("watch failed: %v", err)
			}
		})
	})

	t.Run("passes lastValue to getter", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			lastValues := make(chan int, 2)
			getValue := func(_ fdb.ReadTransaction, _ fdb.KeyConvertible, lastValue int) (int, error) {
				lastValues <- lastValue
				return lastValue + 1, nil
			}

			stateCh, errCh := WatchCh[int](ctx, db, toMonitorKey(db), 41, nil, getValue)

			select {
			case state := <-stateCh:
				assert.Equal(t, 42, state)
			case err := <-errCh:
				t.Fatalf("watch failed: %v", err)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for initial value")
			}

			select {
			case lastValue := <-lastValues:
				assert.Equal(t, 41, lastValue)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for initial lastValue")
			}

			_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
				tx.Set(toMonitorKey(db), []byte("bump"))
				return nil, nil
			})
			assert.NoError(t, err)

			select {
			case state := <-stateCh:
				assert.Equal(t, 43, state)
			case err := <-errCh:
				t.Fatalf("watch failed: %v", err)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for update")
			}

			select {
			case lastValue := <-lastValues:
				assert.Equal(t, 42, lastValue)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for updated lastValue")
			}
		})
	})

	t.Run("detects update after initial value", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			stateCh, errCh := WatchCh[string](t.Context(), db, toMonitorKey(db), "", nil, PrivateEncodingGetter[string])

			// Receive initial (empty)
			select {
			case <-stateCh:
			case err := <-errCh:
				t.Fatalf("watch failed: %v", err)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for initial value")
			}

			// Now update
			err := updateToMonitorToString(db, "newValue")
			assert.NoError(t, err)

			// Should receive the update
			select {
			case state := <-stateCh:
				assert.Equal(t, "newValue", state)
			case err := <-errCh:
				t.Fatalf("watch failed: %v", err)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for update")
			}
		})
	})

	t.Run("detects a delete by sending a zero value", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
				buf := bytes.NewBuffer(nil)
				encoder := privateencoding.NewEncoder[string](buf)
				assert.NoError(t, encoder.Encode("initialValue"))
				tx.Set(toMonitorKey(db), buf.Bytes())
				return nil, nil
			})
			assert.NoError(t, err)
			stateCh, errCh := WatchCh[string](t.Context(), db, toMonitorKey(db), "", nil, PrivateEncodingGetter[string])

			// Receive initial (empty)
			select {
			case <-stateCh:
			case err := <-errCh:
				t.Fatalf("watch failed: %v", err)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for initial value")
			}

			// Now delete
			_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
				tx.Clear(toMonitorKey(db))
				return nil, nil
			})
			assert.NoError(t, err)

			// Should receive zero value
			select {
			case state := <-stateCh:
				assert.Equal(t, "", state)
			case err := <-errCh:
				t.Fatalf("watch failed: %v", err)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for deletion notification")
			}
		})
	})

	t.Run("eventual consistency with rapid updates", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			stateCh, errCh := WatchCh[string](t.Context(), db, toMonitorKey(db), "", nil, PrivateEncodingGetter[string])

			// Wait for initial empty value
			select {
			case state := <-stateCh:
				assert.Equal(t, "", state)
			case err := <-errCh:
				t.Fatalf("watch failed: %v", err)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for initial value")
			}

			// Fire off rapid updates
			for i := range 10 {
				err := updateToMonitorToString(db, fmt.Sprintf("updatedValue%d", i))
				assert.NoError(t, err)
			}

			// Drain until we see the final value (with a timeout)
			deadline := time.After(time.Second * 2)
			var latestValue string
			for latestValue != "updatedValue9" {
				select {
				case latestValue = <-stateCh:
					// keep draining
				case err := <-errCh:
					t.Fatalf("watch failed: %v", err)
				case <-deadline:
					t.Fatalf("timed out waiting for final value, got: %q", latestValue)
				}
			}
			assert.Equal(t, "updatedValue9", latestValue)
		})
	})

	t.Run("key deletion emits zero value", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			// Set an initial value
			err := updateToMonitorToString(db, "initialValue")
			assert.NoError(t, err)

			stateCh, errCh := WatchCh[string](t.Context(), db, toMonitorKey(db), "", nil, PrivateEncodingGetter[string])

			// Receive initial value
			select {
			case state := <-stateCh:
				assert.Equal(t, "initialValue", state)
			case err := <-errCh:
				t.Fatalf("watch failed: %v", err)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for initial value")
			}

			// Delete the key
			_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
				tx.Clear(toMonitorKey(db))
				return nil, nil
			})
			assert.NoError(t, err)

			// Should receive zero value
			select {
			case state := <-stateCh:
				assert.Equal(t, "", state)
			case err := <-errCh:
				t.Fatalf("watch failed: %v", err)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for deletion notification")
			}
		})
	})

	t.Run("long-running watch with periodic updates", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			stateCh, errCh := WatchCh[string](t.Context(), db, toMonitorKey(db), "", nil, PrivateEncodingGetter[string])

			// Receive initial empty value
			select {
			case <-stateCh:
			case err := <-errCh:
				t.Fatalf("watch failed: %v", err)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for initial value")
			}

			// Send multiple updates with small delays and verify each is received
			for i := range 5 {
				expectedValue := fmt.Sprintf("value%d", i)
				err := updateToMonitorToString(db, expectedValue)
				assert.NoError(t, err)

				select {
				case state := <-stateCh:
					assert.Equal(t, expectedValue, state)
				case err := <-errCh:
					t.Fatalf("watch failed on iteration %d: %v", i, err)
				case <-time.After(time.Second):
					t.Fatalf("timeout waiting for update on iteration %d", i)
				}
			}
		})
	})

	t.Run("decode error is sent to error channel", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			// Write garbage bytes that can't be decoded
			_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
				tx.Set(toMonitorKey(db), []byte{0xFF, 0xFE, 0xFD, 0xFC})
				return nil, nil
			})
			assert.NoError(t, err)

			stateCh, errCh := WatchCh[string](t.Context(), db, toMonitorKey(db), "", nil, PrivateEncodingGetter[string])

			select {
			case <-stateCh:
				t.Fatal("expected decode error, got value")
			case err := <-errCh:
				assert.Error(t, err)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for error")
			}
		})
	})

	t.Run("multiple consecutive watches on same key", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			err := updateToMonitorToString(db, "value1")
			assert.NoError(t, err)

			// First watch
			ctx1, cancel1 := context.WithCancel(t.Context())
			stateCh1, errCh1 := WatchCh[string](ctx1, db, toMonitorKey(db), "", nil, PrivateEncodingGetter[string])

			select {
			case state := <-stateCh1:
				assert.Equal(t, "value1", state)
			case err := <-errCh1:
				t.Fatalf("first watch failed: %v", err)
			case <-time.After(time.Second):
				t.Fatal("timeout on first watch")
			}

			// Cancel first watch
			cancel1()

			// Wait for first watch to close
			select {
			case <-errCh1:
				// expected - context cancelled
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for first watch to close")
			}

			// Update value
			err = updateToMonitorToString(db, "value2")
			assert.NoError(t, err)

			// Second watch should see the new value
			stateCh2, errCh2 := WatchCh[string](t.Context(), db, toMonitorKey(db), "", nil, PrivateEncodingGetter[string])

			select {
			case state := <-stateCh2:
				assert.Equal(t, "value2", state)
			case err := <-errCh2:
				t.Fatalf("second watch failed: %v", err)
			case <-time.After(time.Second):
				t.Fatal("timeout on second watch")
			}
		})
	})

	t.Run("watch returns the context error when the context is cancelled", func(t *testing.T) {
		t.Run("before start", func(t *testing.T) {
			testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
				ctx, cancel := context.WithCancel(t.Context())
				cancel()
				stateCh, errCh := WatchCh[string](ctx, db, toMonitorKey(db), "", nil, PrivateEncodingGetter[string])
				select {
				case <-stateCh:
					t.Fatalf("watch should have returned an error")
				case err := <-errCh:
					assert.ErrorIs(t, err, context.Canceled)
				}
			})
		})
		t.Run("while waiting for change", func(t *testing.T) {
			testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
				ctx, cancel := context.WithCancel(t.Context())
				stateCh, errCh := WatchCh[string](ctx, db, toMonitorKey(db), "", nil, PrivateEncodingGetter[string])

				select {
				case <-stateCh:
				case err := <-errCh:
					t.Fatalf("watch failed: %v", err)
				case <-time.After(time.Second):
					t.Fatal("timeout waiting for initial value")
				}

				cancel()

				select {
				case err := <-errCh:
					assert.ErrorIs(t, err, context.Canceled)
				case <-time.After(time.Second):
					t.Fatal("timeout waiting for cancellation")
				}
			})
		})
		t.Run("while waiting on initial watch", func(t *testing.T) {
			testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
				ctx, cancel := context.WithCancel(t.Context())
				initialWatch := newMockFutureNil()
				stateCh, errCh := WatchCh[string](ctx, db, toMonitorKey(db), "", initialWatch, PrivateEncodingGetter[string])

				select {
				case <-initialWatch.Started():
				case <-time.After(time.Second):
					t.Fatal("timeout waiting for initial watch to start")
				}

				cancel()

				select {
				case err := <-errCh:
					assert.ErrorIs(t, err, context.Canceled)
				case <-time.After(time.Second):
					t.Fatal("timeout waiting for cancellation")
				}

				select {
				case value, ok := <-stateCh:
					if ok {
						t.Fatalf("unexpected value before initial watch resolved: %q", value)
					}
				default:
				}
			})
		})
	})
}

func TestWatchExported(t *testing.T) {
	toMonitorKey := func(db util.DbRoot) fdb.Key {
		return db.Root.Pack(tuple.Tuple{"toMonitor"})
	}
	updateToMonitorToString := func(db util.DbRoot, value string) error {
		_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
			buf := bytes.NewBuffer(nil)
			encoder := privateencoding.NewEncoder[string](buf)
			if err := encoder.Encode(value); err != nil {
				return nil, err
			}
			tx.Set(toMonitorKey(db), buf.Bytes())
			return nil, nil
		})
		return err
	}

	t.Run("onChange callback receives initial value", func(t *testing.T) {
		t.Run("without initial value", func(t *testing.T) {
			testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
				onChange := Watch[string](t.Context(), db, toMonitorKey(db), "", nil, PrivateEncodingGetter[string])

				received := make(chan string, 1)
				go func() {
					err := onChange(func(value string) error {
						received <- value
						return nil
					})
					assert.ErrorIs(t, err, t.Context().Err())
				}()

				select {
				case value := <-received:
					assert.Equal(t, "", value)
				case <-time.After(time.Second):
					t.Fatal("timeout waiting for initial value")
				}
			})
		})
		t.Run("with initial value", func(t *testing.T) {
			testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
				const expectedValue = "initialValue"
				err := updateToMonitorToString(db, expectedValue)
				assert.NoError(t, err)

				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()

				onChange := Watch[string](ctx, db, toMonitorKey(db), "", nil, PrivateEncodingGetter[string])

				received := make(chan string, 1)
				errCh := make(chan error, 1)
				go func() {
					err := onChange(func(value string) error {
						select {
						case received <- value:
						default:
						}
						return nil
					})
					errCh <- err
				}()

				select {
				case value := <-received:
					assert.Equal(t, expectedValue, value)
				case err := <-errCh:
					t.Fatalf("onChange returned early with error: %v", err)
				case <-time.After(time.Second):
					t.Fatal("timeout waiting for initial value")
				}
			})
		})
	})

	t.Run("onChange uses lastValue from previous iteration", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			lastValues := make(chan int, 2)
			getValue := func(_ fdb.ReadTransaction, _ fdb.KeyConvertible, lastValue int) (int, error) {
				lastValues <- lastValue
				return lastValue + 1, nil
			}
			onChange := Watch[int](ctx, db, toMonitorKey(db), 10, nil, getValue)

			received := make(chan int, 2)
			errCh := make(chan error, 1)
			go func() {
				err := onChange(func(value int) error {
					received <- value
					return nil
				})
				errCh <- err
			}()

			select {
			case value := <-received:
				assert.Equal(t, 11, value)
			case err := <-errCh:
				t.Fatalf("onChange returned early with error: %v", err)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for initial value")
			}

			select {
			case lastValue := <-lastValues:
				assert.Equal(t, 10, lastValue)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for initial lastValue")
			}

			_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
				tx.Set(toMonitorKey(db), []byte("bump"))
				return nil, nil
			})
			assert.NoError(t, err)

			select {
			case value := <-received:
				assert.Equal(t, 12, value)
			case err := <-errCh:
				t.Fatalf("onChange returned early with error: %v", err)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for updated value")
			}

			select {
			case lastValue := <-lastValues:
				assert.Equal(t, 11, lastValue)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for updated lastValue")
			}

			cancel()
			select {
			case err := <-errCh:
				assert.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for onChange to return")
			}
		})
	})

	t.Run("onChange callback receives updates", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			ctx, cancel := context.WithCancel(t.Context())
			onChange := Watch[string](ctx, db, toMonitorKey(db), "", nil, PrivateEncodingGetter[string])

			received := make(chan string, 10)
			errCh := make(chan error, 1)
			go func() {
				err := onChange(func(value string) error {
					received <- value
					return nil
				})
				errCh <- err
			}()

			// Wait for initial empty value
			select {
			case value := <-received:
				assert.Equal(t, "", value)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for initial value")
			}

			// Update the value
			err := updateToMonitorToString(db, "updatedValue")
			assert.NoError(t, err)

			// Should receive the update
			select {
			case value := <-received:
				assert.Equal(t, "updatedValue", value)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for updated value")
			}

			cancel()

			// Should exit with context error
			select {
			case err := <-errCh:
				assert.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for onChange to return")
			}
		})
	})

	t.Run("onChange error cancels the watch", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			onChange := Watch[string](t.Context(), db, toMonitorKey(db), "", nil, PrivateEncodingGetter[string])

			expectedErr := fmt.Errorf("callback error")
			errCh := make(chan error, 1)
			go func() {
				err := onChange(func(value string) error {
					return expectedErr
				})
				errCh <- err
			}()

			select {
			case err := <-errCh:
				assert.ErrorIs(t, err, expectedErr)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for onChange to return with error")
			}
		})
	})

	t.Run("context cancellation stops the watch", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			ctx, cancel := context.WithCancel(t.Context())
			onChange := Watch[string](ctx, db, toMonitorKey(db), "", nil, PrivateEncodingGetter[string])

			errCh := make(chan error, 1)
			started := make(chan struct{})
			go func() {
				err := onChange(func(value string) error {
					close(started)
					return nil
				})
				errCh <- err
			}()

			// Wait for initial callback
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for callback to start")
			}

			// Cancel the context
			cancel()

			select {
			case err := <-errCh:
				assert.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for context cancellation")
			}
		})
	})

	t.Run("decode error propagates to onChange caller", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			// Write garbage bytes that can't be decoded
			_, err := db.Transact(func(tx fdb.Transaction) (any, error) {
				tx.Set(toMonitorKey(db), []byte{0xFF, 0xFE, 0xFD, 0xFC})
				return nil, nil
			})
			assert.NoError(t, err)

			onChange := Watch[string](t.Context(), db, toMonitorKey(db), "", nil, PrivateEncodingGetter[string])

			errCh := make(chan error, 1)
			unexpectedValue := make(chan struct{}, 1)
			go func() {
				err := onChange(func(value string) error {
					unexpectedValue <- struct{}{}
					return nil
				})
				errCh <- err
			}()

			select {
			case err := <-errCh:
				assert.Error(t, err)
			case <-unexpectedValue:
				t.Fatal("should not receive value on decode error")
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for decode error")
			}
		})
	})

	t.Run("multiple values received in sequence", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			onChange := Watch[string](ctx, db, toMonitorKey(db), "", nil, PrivateEncodingGetter[string])

			received := make(chan string, 20)
			go func() {
				_ = onChange(func(value string) error {
					received <- value
					return nil
				})
			}()

			// Wait for initial empty value
			select {
			case value := <-received:
				assert.Equal(t, "", value)
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for initial value")
			}

			// Send multiple updates
			for i := range 5 {
				err := updateToMonitorToString(db, fmt.Sprintf("value%d", i))
				assert.NoError(t, err)
			}

			// Drain until we see the final value
			deadline := time.After(time.Second * 2)
			var latestValue string
			for latestValue != "value4" {
				select {
				case latestValue = <-received:
				case <-deadline:
					t.Fatalf("timed out waiting for final value, got: %q", latestValue)
				}
			}
			assert.Equal(t, "value4", latestValue)
		})
	})

	t.Run("callback runs concurrently with watch loop", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			onChange := Watch[string](ctx, db, toMonitorKey(db), "", nil, PrivateEncodingGetter[string])

			callbackStarted := make(chan struct{})
			callbackDone := make(chan struct{})
			go func() {
				_ = onChange(func(value string) error {
					select {
					case callbackStarted <- struct{}{}:
					default:
					}
					// Simulate slow callback
					time.Sleep(50 * time.Millisecond)
					select {
					case callbackDone <- struct{}{}:
					default:
					}
					return nil
				})
			}()

			// Wait for first callback to start
			select {
			case <-callbackStarted:
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for callback to start")
			}

			// Wait for callback to complete
			select {
			case <-callbackDone:
			case <-time.After(time.Second):
				t.Fatal("timeout waiting for callback to complete")
			}
		})
	})
}
