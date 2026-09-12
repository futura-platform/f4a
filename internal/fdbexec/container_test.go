package fdbexec_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"slices"
	"sync"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/f4a/internal/fdbexec"
	"github.com/futura-platform/f4a/internal/task"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	testutil "github.com/futura-platform/f4a/internal/util/test"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/futura-platform/futura/moment"
	"github.com/futura-platform/futura/privateencoding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createTaskAndContainer(t *testing.T, db dbutil.DbRoot, id task.Id) *fdbexec.ExecutionContainer {
	tasks, err := task.CreateOrOpenTasksDirectory(db)
	require.NoError(t, err)
	_, err = tasks.Create(db, id)
	require.NoError(t, err)

	tkey, err := tasks.Open(db, id)
	require.NoError(t, err)

	return fdbexec.OpenTaskContainer(db, tkey, "user")
}

func TestExecutionContainer(t *testing.T) {
	gob.Register(struct{}{})

	assert.NotPanics(t, func() {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			assert.NotNil(t, createTaskAndContainer(t, db, task.NewId()))
		})
	})

	ephemeralTransactTest := func(
		t *testing.T,
		txFns ...func(ctx context.Context, tx executiontype.Container) error,
	) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			container := createTaskAndContainer(t, db, task.NewId())
			for _, txFn := range txFns {
				err := container.Transact(t.Context(), txFn)
				assert.NoError(t, err)
				if err != nil {
					return
				}
			}
		})
	}

	testIdentity := moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "test.go", Line: 1}}), moment.Callsite{File: "fn.go", Line: 1})
	t.Run("Transact", func(t *testing.T) {
		t.Run("Error rolls back transaction", func(t *testing.T) {
			testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
				container := createTaskAndContainer(t, db, task.NewId())
				errSentinel := errors.New("boom")

				err := container.Transact(t.Context(), func(ctx context.Context, tx executiontype.Container) error {
					tx.SetMoment(testIdentity, []byte("moment"))
					return errSentinel
				})
				assert.ErrorIs(t, err, errSentinel)

				err = container.Transact(t.Context(), func(ctx context.Context, tx executiontype.Container) error {
					assert.False(t, tx.HasMoment(testIdentity))
					return nil
				})
				assert.NoError(t, err)
			})
		})
		t.Run("CallOrder", func(t *testing.T) {
			t.Run("Starts with 0 length", func(t *testing.T) {
				ephemeralTransactTest(t, func(ctx context.Context, tx executiontype.Container) error {
					assert.Equal(t, 0, tx.CallOrderLength())
					return nil
				})
			})
			t.Run("Can append to call order", func(t *testing.T) {
				ephemeralTransactTest(t, func(ctx context.Context, tx executiontype.Container) error {
					tx.AppendCallOrder(testIdentity)
					return nil
				}, func(ctx context.Context, tx executiontype.Container) error {
					assert.Equal(t, 1, tx.CallOrderLength())
					assert.Equal(t, testIdentity, tx.CallOrderAt(0))
					return nil
				})
			})
			t.Run("Can set call order at index", func(t *testing.T) {
				t.Run("Panics if index is out of bounds", func(t *testing.T) {
					ephemeralTransactTest(t, func(ctx context.Context, tx executiontype.Container) error {
						// fn runs against the database and then against memory: the
						// database panics with ErrOutOfBounds, memory with Go's own
						assert.Panics(t, func() { tx.CallOrderAt(0) })
						assert.Panics(t, func() { tx.CallOrderAt(1) })
						assert.Panics(t, func() { tx.CallOrderAt(-1) })
						assert.Panics(t, func() { tx.SetCallOrderAt(1, testIdentity) })
						assert.Panics(t, func() { tx.SetCallOrderAt(-1, testIdentity) })
						return nil
					})
				})
				t.Run("Can set call order at index", func(t *testing.T) {
					ephemeralTransactTest(t, func(ctx context.Context, tx executiontype.Container) error {
						notTestIdentity := moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "test.go", Line: 2}}), moment.Callsite{File: "fn.go", Line: 2})
						tx.AppendCallOrder(notTestIdentity)
						tx.SetCallOrderAt(0, testIdentity)
						return nil
					}, func(ctx context.Context, tx executiontype.Container) error {
						assert.Equal(t, testIdentity, tx.CallOrderAt(0))
						return nil
					})
				})
				t.Run("Does not change length or other indices", func(t *testing.T) {
					firstIdentity := moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "test.go", Line: 4}}), moment.Callsite{File: "fn.go", Line: 4})
					secondIdentity := moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "test.go", Line: 5}}), moment.Callsite{File: "fn.go", Line: 5})
					updatedIdentity := moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "test.go", Line: 6}}), moment.Callsite{File: "fn.go", Line: 6})
					ephemeralTransactTest(t, func(ctx context.Context, tx executiontype.Container) error {
						tx.AppendCallOrder(firstIdentity)
						tx.AppendCallOrder(secondIdentity)
						tx.SetCallOrderAt(0, updatedIdentity)
						return nil
					}, func(ctx context.Context, tx executiontype.Container) error {
						assert.Equal(t, 2, tx.CallOrderLength())
						assert.Equal(t, updatedIdentity, tx.CallOrderAt(0))
						assert.Equal(t, secondIdentity, tx.CallOrderAt(1))
						return nil
					})
				})
			})
			t.Run("Append preserves order within transaction", func(t *testing.T) {
				firstIdentity := moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "test.go", Line: 7}}), moment.Callsite{File: "fn.go", Line: 7})
				secondIdentity := moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "test.go", Line: 8}}), moment.Callsite{File: "fn.go", Line: 8})
				thirdIdentity := moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "test.go", Line: 9}}), moment.Callsite{File: "fn.go", Line: 9})
				ephemeralTransactTest(t, func(ctx context.Context, tx executiontype.Container) error {
					tx.AppendCallOrder(firstIdentity)
					tx.AppendCallOrder(secondIdentity)
					tx.AppendCallOrder(thirdIdentity)
					return nil
				}, func(ctx context.Context, tx executiontype.Container) error {
					assert.Equal(t, 3, tx.CallOrderLength())
					assert.Equal(t, firstIdentity, tx.CallOrderAt(0))
					assert.Equal(t, secondIdentity, tx.CallOrderAt(1))
					assert.Equal(t, thirdIdentity, tx.CallOrderAt(2))
					return nil
				})
			})
			t.Run("Truncate", func(t *testing.T) {
				identities := make([]moment.Identity, 3)
				for i := range identities {
					identities[i] = moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "test.go", Line: 20 + i}}), moment.Callsite{File: "fn.go", Line: 20 + i})
				}
				appendAll := func(ctx context.Context, tx executiontype.Container) error {
					for _, identity := range identities {
						tx.AppendCallOrder(identity)
					}
					return nil
				}
				t.Run("keeps the entries up to index", func(t *testing.T) {
					ephemeralTransactTest(t, appendAll, func(ctx context.Context, tx executiontype.Container) error {
						tx.TruncateCallOrderAt(1)
						return nil
					}, func(ctx context.Context, tx executiontype.Container) error {
						assert.Equal(t, 2, tx.CallOrderLength())
						assert.Equal(t, identities[0], tx.CallOrderAt(0))
						assert.Equal(t, identities[1], tx.CallOrderAt(1))
						assert.Panics(t, func() { tx.CallOrderAt(2) })
						return nil
					})
				})
				t.Run("an append after a truncate lands at the new end", func(t *testing.T) {
					ephemeralTransactTest(t, appendAll, func(ctx context.Context, tx executiontype.Container) error {
						tx.TruncateCallOrderAt(0)
						tx.AppendCallOrder(identities[2])
						return nil
					}, func(ctx context.Context, tx executiontype.Container) error {
						assert.Equal(t, 2, tx.CallOrderLength())
						assert.Equal(t, identities[0], tx.CallOrderAt(0))
						assert.Equal(t, identities[2], tx.CallOrderAt(1))
						return nil
					})
				})
				t.Run("index -1 empties the call order", func(t *testing.T) {
					ephemeralTransactTest(t, appendAll, func(ctx context.Context, tx executiontype.Container) error {
						tx.TruncateCallOrderAt(-1)
						return nil
					}, func(ctx context.Context, tx executiontype.Container) error {
						assert.Equal(t, 0, tx.CallOrderLength())
						assert.Panics(t, func() { tx.CallOrderAt(0) })
						return nil
					})
				})
				t.Run("an index at or past the end changes nothing", func(t *testing.T) {
					ephemeralTransactTest(t, appendAll, func(ctx context.Context, tx executiontype.Container) error {
						tx.TruncateCallOrderAt(2)
						tx.TruncateCallOrderAt(7)
						return nil
					}, func(ctx context.Context, tx executiontype.Container) error {
						assert.Equal(t, 3, tx.CallOrderLength())
						assert.Equal(t, identities[2], tx.CallOrderAt(2))
						return nil
					})
				})
			})
		})
		testMoment := []byte("moment")
		t.Run("Memo Table", func(t *testing.T) {
			ephemeralTransactTest(t, func(ctx context.Context, tx executiontype.Container) error {
				assert.False(t, tx.HasMoment(testIdentity))
				m, ok := tx.GetMoment(testIdentity)
				assert.False(t, ok)
				assert.Zero(t, m)
				tx.SetMoment(testIdentity, testMoment)
				return nil
			}, func(ctx context.Context, tx executiontype.Container) error {
				assert.True(t, tx.HasMoment(testIdentity))
				m, ok := tx.GetMoment(testIdentity)
				assert.True(t, ok)
				assert.Equal(t, testMoment, m)

				tx.DeleteMoment(testIdentity)
				return nil
			}, func(ctx context.Context, tx executiontype.Container) error {
				assert.False(t, tx.HasMoment(testIdentity))
				m, ok := tx.GetMoment(testIdentity)
				assert.False(t, ok)
				assert.Zero(t, m)
				return nil
			})
			t.Run("HasMoment for other identity is false", func(t *testing.T) {
				otherIdentity := moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "test.go", Line: 10}}), moment.Callsite{File: "fn.go", Line: 10})
				ephemeralTransactTest(t, func(ctx context.Context, tx executiontype.Container) error {
					tx.SetMoment(testIdentity, testMoment)
					return nil
				}, func(ctx context.Context, tx executiontype.Container) error {
					assert.False(t, tx.HasMoment(otherIdentity))
					return nil
				})
			})
			t.Run("Delete is idempotent", func(t *testing.T) {
				otherIdentity := moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "test.go", Line: 11}}), moment.Callsite{File: "fn.go", Line: 11})
				ephemeralTransactTest(t, func(ctx context.Context, tx executiontype.Container) error {
					tx.SetMoment(testIdentity, testMoment)
					tx.DeleteMoment(otherIdentity)
					return nil
				}, func(ctx context.Context, tx executiontype.Container) error {
					assert.True(t, tx.HasMoment(testIdentity))
					assert.False(t, tx.HasMoment(otherIdentity))
					tx.DeleteMoment(otherIdentity)
					return nil
				})
			})
		})
		t.Run("KnownMoments", func(t *testing.T) {
			t.Run("normal usage", func(t *testing.T) {
				secondIdentity := moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "test.go", Line: 3}}), moment.Callsite{File: "fn.go", Line: 3})
				ephemeralTransactTest(t, func(ctx context.Context, tx executiontype.Container) error {
					// Initially empty
					count := 0
					for range tx.KnownMoments() {
						count++
					}
					assert.Equal(t, 0, count)

					// Add some moments
					tx.SetMoment(testIdentity, testMoment)
					tx.SetMoment(secondIdentity, testMoment)
					return nil
				}, func(ctx context.Context, tx executiontype.Container) error {
					// Should iterate over both identities
					seen := make(map[moment.Identity]bool)
					for id := range tx.KnownMoments() {
						seen[id] = true
					}
					assert.Len(t, seen, 2)
					assert.True(t, seen[testIdentity])
					assert.True(t, seen[secondIdentity])

					// Delete one moment
					tx.DeleteMoment(testIdentity)
					return nil
				}, func(ctx context.Context, tx executiontype.Container) error {
					// Should only have one identity now
					seen := make(map[moment.Identity]bool)
					for id := range tx.KnownMoments() {
						seen[id] = true
					}
					assert.Len(t, seen, 1)
					assert.False(t, seen[testIdentity])
					assert.True(t, seen[secondIdentity])
					return nil
				})
			})
			t.Run("iteration", func(t *testing.T) {
				t.Run("with no moments", func(t *testing.T) {
					ephemeralTransactTest(t, func(ctx context.Context, tx executiontype.Container) error {
						return nil
					}, func(ctx context.Context, tx executiontype.Container) error {
						moments := make([]moment.Identity, 0)
						for m := range tx.KnownMoments() {
							moments = append(moments, m)
						}
						assert.Equal(t, []moment.Identity{}, moments)
						return nil
					})
				})
				t.Run("with many moments", func(t *testing.T) {
					srcMoments := mapset.NewSet[moment.Identity]()
					for i := range 10 {
						srcMoments.Add(moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "test.go", Line: i}}), moment.Callsite{File: "fn.go", Line: i}))
					}
					ephemeralTransactTest(t, func(ctx context.Context, tx executiontype.Container) error {
						for m := range srcMoments.Iter() {
							tx.SetMoment(m, testMoment)
						}
						return nil
					}, func(ctx context.Context, tx executiontype.Container) error {
						moments := make([]moment.Identity, 0)
						for m := range tx.KnownMoments() {
							moments = append(moments, m)
						}
						assert.Equal(t, srcMoments.Cardinality(), len(moments))
						for _, m := range moments {
							assert.True(t, srcMoments.Contains(m))
						}
						return nil
					})
				})
			})
			t.Run("duplicate key behaviour", func(t *testing.T) {
				ephemeralTransactTest(t, func(ctx context.Context, tx executiontype.Container) error {
					tx.SetMoment(testIdentity, testMoment)
					tx.SetMoment(testIdentity, testMoment)
					return nil
				}, func(ctx context.Context, tx executiontype.Container) error {
					moments := make([]moment.Identity, 0)
					for m := range tx.KnownMoments() {
						moments = append(moments, m)
					}
					assert.Equal(t, []moment.Identity{testIdentity}, moments)
					return nil
				})
			})
		})
		t.Run("Isolation per task id", func(t *testing.T) {
			testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
				firstContainer := createTaskAndContainer(t, db, task.NewId())
				secondContainer := createTaskAndContainer(t, db, task.NewId())

				err := firstContainer.Transact(t.Context(), func(ctx context.Context, tx executiontype.Container) error {
					tx.SetMoment(testIdentity, testMoment)
					return nil
				})
				assert.NoError(t, err)

				err = secondContainer.Transact(t.Context(), func(ctx context.Context, tx executiontype.Container) error {
					assert.False(t, tx.HasMoment(testIdentity))
					return nil
				})
				assert.NoError(t, err)
			})
		})
	})
}

// rawKeys reads every key under the container's subspaces straight from the
// database, bypassing the image.
func rawKeys(t *testing.T, db dbutil.DbRoot, tkey task.TaskKey, namespace string) map[string][]byte {
	t.Helper()
	out := make(map[string][]byte)
	_, err := db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
		memo, err := tkey.MemoTable(db, namespace)
		require.NoError(t, err)
		order, err := tkey.CallOrder(db, namespace)
		require.NoError(t, err)
		durable, err := tkey.DurableObjectSpace(db, namespace)
		require.NoError(t, err)
		for _, sub := range []directory.DirectorySubspace{memo, order, durable} {
			begin, end := sub.FDBRangeKeys()
			for _, kv := range tx.GetRange(fdb.KeyRange{Begin: begin, End: end}, fdb.RangeOptions{}).GetSliceOrPanic() {
				out[string(kv.Key)] = kv.Value
			}
		}
		return nil, nil
	})
	require.NoError(t, err)
	return out
}

func TestExecutionContainerCache(t *testing.T) {
	testMoment := []byte("moment")
	identities := func(n int) []moment.Identity {
		out := make([]moment.Identity, n)
		for i := range out {
			out[i] = moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "cache_test.go", Line: i}}), moment.Callsite{File: "fn.go", Line: i})
		}
		return out
	}

	t.Run("a second container over the same task loads what the first committed", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			id := task.NewId()
			first := createTaskAndContainer(t, db, id)
			ids := identities(3)
			require.NoError(t, first.Transact(t.Context(), func(ctx context.Context, tx executiontype.Container) error {
				for _, identity := range ids {
					tx.AppendCallOrder(identity)
					tx.SetMoment(identity, testMoment)
				}
				tx.SetCallOrderAt(1, ids[0])
				tx.DeleteMoment(ids[2])
				return tx.StoreDurable("k", []byte("v"))
			}))
			require.NoError(t, first.Transact(t.Context(), func(ctx context.Context, tx executiontype.Container) error {
				tx.TruncateCallOrderAt(1)
				return nil
			}))

			require.NoError(t, reopen(t, db, id).ReadTransact(t.Context(), func(ctx context.Context, tx executiontype.ReadOnlyContainer) error {
				assert.Equal(t, 2, tx.CallOrderLength())
				assert.Equal(t, ids[0], tx.CallOrderAt(0))
				assert.Equal(t, ids[0], tx.CallOrderAt(1))
				assert.Panics(t, func() { tx.CallOrderAt(2) })
				assert.True(t, tx.HasMoment(ids[0]))
				assert.True(t, tx.HasMoment(ids[1]))
				assert.False(t, tx.HasMoment(ids[2]))
				m, ok := tx.GetMoment(ids[1])
				assert.True(t, ok)
				assert.Equal(t, testMoment, m)
				assert.ElementsMatch(t, ids[:2], slices.Collect(tx.KnownMoments()))
				v, ok, err := tx.LoadDurable("k")
				require.NoError(t, err)
				assert.True(t, ok)
				assert.Equal(t, []byte("v"), v)
				return nil
			}))
		})
	})

	t.Run("reads are served from memory", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			id := task.NewId()
			container := createTaskAndContainer(t, db, id)
			ids := identities(1)
			require.NoError(t, container.Transact(t.Context(), func(ctx context.Context, tx executiontype.Container) error {
				tx.AppendCallOrder(ids[0])
				return tx.StoreDurable("k", []byte("v"))
			}))

			// a write behind the container's back: it must not be observed
			tasks, err := task.CreateOrOpenTasksDirectory(db)
			require.NoError(t, err)
			tkey, err := tasks.Open(db, id)
			require.NoError(t, err)
			durable, err := tkey.DurableObjectSpace(db, "user")
			require.NoError(t, err)
			order, err := tkey.CallOrder(db, "user")
			require.NoError(t, err)
			_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
				tx.Set(durable.Pack(tuple.Tuple{"k"}), []byte("stale"))
				begin, end := order.FDBRangeKeys()
				tx.ClearRange(fdb.KeyRange{Begin: begin, End: end})
				return nil, nil
			})
			require.NoError(t, err)

			require.NoError(t, container.ReadTransact(t.Context(), func(ctx context.Context, tx executiontype.ReadOnlyContainer) error {
				v, ok, err := tx.LoadDurable("k")
				require.NoError(t, err)
				assert.True(t, ok)
				assert.Equal(t, []byte("v"), v)
				assert.Equal(t, 1, tx.CallOrderLength())
				return nil
			}))

			// a container loading its own image sees the database as it is
			require.NoError(t, reopen(t, db, id).ReadTransact(t.Context(), func(ctx context.Context, tx executiontype.ReadOnlyContainer) error {
				v, _, _ := tx.LoadDurable("k")
				assert.Equal(t, []byte("stale"), v)
				assert.Equal(t, 0, tx.CallOrderLength())
				return nil
			}))
		})
	})

	t.Run("a failed transaction leaves neither the database nor the image changed", func(t *testing.T) {
		for _, fail := range []struct {
			name string
			fn   func(tx executiontype.Container)
		}{
			{"error", func(executiontype.Container) {}},
			{"panic", func(executiontype.Container) { panic("boom") }},
		} {
			t.Run(fail.name, func(t *testing.T) {
				testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
					id := task.NewId()
					container := createTaskAndContainer(t, db, id)
					ids := identities(2)
					require.NoError(t, container.Transact(t.Context(), func(ctx context.Context, tx executiontype.Container) error {
						tx.AppendCallOrder(ids[0])
						tx.SetMoment(ids[0], testMoment)
						return tx.StoreDurable("k", []byte("v"))
					}))
					before := rawKeys(t, db, createTaskKey(t, db, id), "user")

					errSentinel := errors.New("rolled back")
					run := func() error {
						return container.Transact(t.Context(), func(ctx context.Context, tx executiontype.Container) error {
							tx.AppendCallOrder(ids[1])
							tx.SetCallOrderAt(0, ids[1])
							tx.SetMoment(ids[1], testMoment)
							tx.DeleteMoment(ids[0])
							_ = tx.StoreDurable("k", []byte("v2"))
							fail.fn(tx)
							return errSentinel
						})
					}
					if fail.name == "panic" {
						assert.PanicsWithValue(t, "boom", func() { _ = run() })
					} else {
						assert.ErrorIs(t, run(), errSentinel)
					}

					assert.Equal(t, before, rawKeys(t, db, createTaskKey(t, db, id), "user"))
					check := func(tx executiontype.ReadOnlyContainer) {
						assert.Equal(t, 1, tx.CallOrderLength())
						assert.Equal(t, ids[0], tx.CallOrderAt(0))
						assert.True(t, tx.HasMoment(ids[0]))
						assert.False(t, tx.HasMoment(ids[1]))
						v, _, _ := tx.LoadDurable("k")
						assert.Equal(t, []byte("v"), v)
					}
					require.NoError(t, container.ReadTransact(t.Context(), func(ctx context.Context, tx executiontype.ReadOnlyContainer) error {
						check(tx)
						return nil
					}))
					require.NoError(t, container.Transact(t.Context(), func(ctx context.Context, tx executiontype.Container) error {
						check(tx)
						return nil
					}))
				})
			})
		}
	})

	t.Run("writes within a transaction are read back before commit", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			container := createTaskAndContainer(t, db, task.NewId())
			ids := identities(2)
			require.NoError(t, container.Transact(t.Context(), func(ctx context.Context, tx executiontype.Container) error {
				tx.AppendCallOrder(ids[0])
				assert.Equal(t, 1, tx.CallOrderLength())
				assert.Equal(t, ids[0], tx.CallOrderAt(0))
				tx.SetMoment(ids[1], testMoment)
				assert.True(t, tx.HasMoment(ids[1]))
				assert.ElementsMatch(t, ids[1:], slices.Collect(tx.KnownMoments()))
				require.NoError(t, tx.StoreDurable("k", []byte("v")))
				v, ok, err := tx.LoadDurable("k")
				require.NoError(t, err)
				assert.True(t, ok)
				assert.Equal(t, []byte("v"), v)
				return nil
			}))
		})
	})

	t.Run("concurrent reads during writes", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			container := createTaskAndContainer(t, db, task.NewId())
			ids := identities(20)
			var wg sync.WaitGroup
			for range 4 {
				wg.Go(func() {
					for range 50 {
						assert.NoError(t, container.ReadTransact(t.Context(), func(ctx context.Context, tx executiontype.ReadOnlyContainer) error {
							// the call order and memo table are always consistent with each other
							n := tx.CallOrderLength()
							for i := range n {
								assert.True(t, tx.HasMoment(tx.CallOrderAt(i)))
							}
							return nil
						}))
					}
				})
			}
			for _, identity := range ids {
				require.NoError(t, container.Transact(t.Context(), func(ctx context.Context, tx executiontype.Container) error {
					tx.SetMoment(identity, testMoment)
					tx.AppendCallOrder(identity)
					return nil
				}))
			}
			wg.Wait()
			require.NoError(t, container.ReadTransact(t.Context(), func(ctx context.Context, tx executiontype.ReadOnlyContainer) error {
				assert.Equal(t, len(ids), tx.CallOrderLength())
				return nil
			}))
		})
	})

	t.Run("loads a large state", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			id := task.NewId()
			container := createTaskAndContainer(t, db, id)
			// well past the load batch size, so the loader pages
			ids := identities(1000)
			for chunk := range slices.Chunk(ids, 100) {
				require.NoError(t, container.Transact(t.Context(), func(ctx context.Context, tx executiontype.Container) error {
					for _, identity := range chunk {
						tx.AppendCallOrder(identity)
						tx.SetMoment(identity, testMoment)
					}
					return nil
				}))
			}
			require.NoError(t, reopen(t, db, id).ReadTransact(t.Context(), func(ctx context.Context, tx executiontype.ReadOnlyContainer) error {
				assert.Equal(t, len(ids), tx.CallOrderLength())
				for i, identity := range ids {
					assert.Equal(t, identity, tx.CallOrderAt(i))
				}
				assert.Len(t, slices.Collect(tx.KnownMoments()), len(ids))
				return nil
			}))
		})
	})
}

// reopen opens another container over the task, with an image of its own.
func reopen(t *testing.T, db dbutil.DbRoot, id task.Id) *fdbexec.ExecutionContainer {
	t.Helper()
	return fdbexec.OpenTaskContainer(db, createTaskKey(t, db, id), "user")
}

func createTaskKey(t *testing.T, db dbutil.DbRoot, id task.Id) task.TaskKey {
	t.Helper()
	tasks, err := task.CreateOrOpenTasksDirectory(db)
	require.NoError(t, err)
	tkey, err := tasks.Open(db, id)
	require.NoError(t, err)
	return tkey
}

func TestExecutionContainerRetry(t *testing.T) {
	testMoment := []byte("moment")
	ids := make([]moment.Identity, 2)
	for i := range ids {
		ids[i] = moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "retry_test.go", Line: i}}), moment.Callsite{File: "fn.go", Line: i})
	}

	t.Run("a retried transaction lands in the image as it committed", func(t *testing.T) {
		testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
			id := task.NewId()
			container := createTaskAndContainer(t, db, id)
			// fn is replay safe: it writes the same thing however often it runs,
			// and the conflict is the database's, not the callback's
			conflicted := false
			calls := 0
			require.NoError(t, container.Transact(t.Context(), func(ctx context.Context, tx executiontype.Container) error {
				calls++
				assert.Equal(t, 0, tx.CallOrderLength())
				tx.AppendCallOrder(ids[0])
				tx.SetMoment(ids[0], testMoment)
				if err := tx.StoreDurable("k", []byte("v")); err != nil {
					return err
				}
				if !conflicted {
					conflicted = true
					return fdb.Error{Code: 1020} // not_committed: the database runs fn again
				}
				return nil
			}))
			// two database attempts, then the replay over the image
			require.Equal(t, 3, calls)

			check := func(tx executiontype.ReadOnlyContainer) {
				assert.Equal(t, 1, tx.CallOrderLength())
				assert.Equal(t, ids[0], tx.CallOrderAt(0))
				assert.True(t, tx.HasMoment(ids[0]))
				v, ok, err := tx.LoadDurable("k")
				require.NoError(t, err)
				assert.True(t, ok)
				assert.Equal(t, []byte("v"), v)
			}
			// the image
			require.NoError(t, container.ReadTransact(t.Context(), func(ctx context.Context, tx executiontype.ReadOnlyContainer) error {
				check(tx)
				return nil
			}))
			// the database
			require.NoError(t, reopen(t, db, id).ReadTransact(t.Context(), func(ctx context.Context, tx executiontype.ReadOnlyContainer) error {
				check(tx)
				return nil
			}))
		})
	})
}

// TestExecutionContainerLoadsLegacyLayout writes the on-disk layout the way the
// previous implementation did (the length key maintained with an atomic add)
// and checks the loader reads it back.
func TestExecutionContainerLoadsLegacyLayout(t *testing.T) {
	privateencoding.Register[struct{}]()
	m := moment.NewMoment(struct{}{})
	m.SetValidOutput(struct{}{})
	testMoment, err := m.Encode()
	require.NoError(t, err)
	ids := make([]moment.Identity, 3)
	for i := range ids {
		ids[i] = moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "legacy_test.go", Line: i}}), moment.Callsite{File: "fn.go", Line: i})
	}
	encode := func(identity moment.Identity) []byte {
		var buf bytes.Buffer
		require.NoError(t, privateencoding.NewEncoder[moment.Identity](&buf).Encode(identity))
		return buf.Bytes()
	}

	testutil.WithEphemeralDBRoot(t, func(db dbutil.DbRoot) {
		id := task.NewId()
		tasks, err := task.CreateOrOpenTasksDirectory(db)
		require.NoError(t, err)
		_, err = tasks.Create(db, id)
		require.NoError(t, err)
		tkey, err := tasks.Open(db, id)
		require.NoError(t, err)
		memo, err := tkey.MemoTable(db, "user")
		require.NoError(t, err)
		order, err := tkey.CallOrder(db, "user")
		require.NoError(t, err)
		durable, err := tkey.DurableObjectSpace(db, "user")
		require.NoError(t, err)

		one := make([]byte, 8)
		binary.LittleEndian.PutUint64(one, 1)
		_, err = db.Transact(func(tx fdb.Transaction) (any, error) {
			for i, identity := range ids {
				tx.Set(order.Pack(tuple.Tuple{i}), encode(identity))
				tx.Add(order.Pack(tuple.Tuple{"length"}), one)
			}
			// the memo table holds the first two; the third was deleted
			tx.Set(memo.Pack(tuple.Tuple{encode(ids[0])}), testMoment)
			tx.Set(memo.Pack(tuple.Tuple{encode(ids[1])}), testMoment)
			tx.Set(durable.Pack(tuple.Tuple{"k"}), []byte("v"))
			tx.Set(durable.Pack(tuple.Tuple{"empty"}), []byte{})
			return nil, nil
		})
		require.NoError(t, err)

		container := fdbexec.OpenTaskContainer(db, tkey, "user")
		require.NoError(t, container.ReadTransact(t.Context(), func(ctx context.Context, tx executiontype.ReadOnlyContainer) error {
			assert.Equal(t, 3, tx.CallOrderLength())
			for i, identity := range ids {
				assert.Equal(t, identity, tx.CallOrderAt(i))
			}
			assert.ElementsMatch(t, ids[:2], slices.Collect(tx.KnownMoments()))
			m, ok := tx.GetMoment(ids[0])
			assert.True(t, ok)
			assert.Equal(t, testMoment, m)
			assert.False(t, tx.HasMoment(ids[2]))
			v, ok, err := tx.LoadDurable("k")
			require.NoError(t, err)
			assert.True(t, ok)
			assert.Equal(t, []byte("v"), v)
			empty, ok, err := tx.LoadDurable("empty")
			require.NoError(t, err)
			assert.True(t, ok)
			assert.NotNil(t, empty)
			assert.Empty(t, empty)
			_, ok, err = tx.LoadDurable("missing")
			require.NoError(t, err)
			assert.False(t, ok)
			return nil
		}))

		// and a write through the container produces keys the legacy layout reads back
		require.NoError(t, container.Transact(t.Context(), func(ctx context.Context, tx executiontype.Container) error {
			tx.TruncateCallOrderAt(0)
			tx.AppendCallOrder(ids[2])
			return nil
		}))
		_, err = db.ReadTransact(func(tx fdb.ReadTransaction) (any, error) {
			length := tx.Get(order.Pack(tuple.Tuple{"length"})).MustGet()
			assert.Equal(t, uint64(2), binary.LittleEndian.Uint64(length))
			assert.Equal(t, encode(ids[0]), tx.Get(order.Pack(tuple.Tuple{0})).MustGet())
			assert.Equal(t, encode(ids[2]), tx.Get(order.Pack(tuple.Tuple{1})).MustGet())
			assert.Nil(t, tx.Get(order.Pack(tuple.Tuple{2})).MustGet())
			return nil, nil
		})
		require.NoError(t, err)
	})
}
