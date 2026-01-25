package fdbexec_test

import (
	"context"
	"encoding/gob"
	"errors"
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/futura-platform/f4a/internal/fdbexec"
	"github.com/futura-platform/f4a/internal/task"
	"github.com/futura-platform/f4a/pkg/util"
	testutil "github.com/futura-platform/f4a/pkg/util/test"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/futura-platform/futura/moment"
	"github.com/stretchr/testify/assert"
)

func TestExecutionContainer(t *testing.T) {
	gob.Register(struct{}{})

	assert.NotPanics(t, func() {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			assert.NotNil(t, fdbexec.NewContainer(task.NewId(), db))
		})
	})

	ephemeralTransactTest := func(
		t *testing.T,
		txFns ...func(ctx context.Context, tx executiontype.Container) error,
	) {
		testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
			container := fdbexec.NewContainer(task.NewId(), db)
			for _, txFn := range txFns {
				err := container.Transact(t.Context(), txFn)
				assert.NoError(t, err)
				if err != nil {
					return
				}
			}
		})
	}

	testIdentity := moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "test.go", Line: 1}}))
	t.Run("Transact", func(t *testing.T) {
		t.Run("Error rolls back transaction", func(t *testing.T) {
			testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
				container := fdbexec.NewContainer(task.NewId(), db)
				errSentinel := errors.New("boom")

				err := container.Transact(t.Context(), func(ctx context.Context, tx executiontype.Container) error {
					testMoment := moment.NewMoment(
						moment.NewFn[struct{}, struct{}](func(ctx context.Context, args struct{}) (struct{}, error) {
							return struct{}{}, nil
						}),
						struct{}{},
					)
					tx.SetMoment(testIdentity, *testMoment)
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
						assert.PanicsWithValue(t, fdbexec.ErrOutOfBounds, func() {
							tx.CallOrderAt(0)
						})
						assert.PanicsWithValue(t, fdbexec.ErrOutOfBounds, func() {
							tx.CallOrderAt(1)
						})
						assert.PanicsWithValue(t, fdbexec.ErrOutOfBounds, func() {
							tx.CallOrderAt(-1)
						})
						assert.PanicsWithValue(t, fdbexec.ErrOutOfBounds, func() {
							tx.SetCallOrderAt(1, testIdentity)
						})
						assert.PanicsWithValue(t, fdbexec.ErrOutOfBounds, func() {
							tx.SetCallOrderAt(-1, testIdentity)
						})
						return nil
					})
				})
				t.Run("Can set call order at index", func(t *testing.T) {
					ephemeralTransactTest(t, func(ctx context.Context, tx executiontype.Container) error {
						notTestIdentity := moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "test.go", Line: 2}}))
						tx.AppendCallOrder(notTestIdentity)
						tx.SetCallOrderAt(0, testIdentity)
						return nil
					}, func(ctx context.Context, tx executiontype.Container) error {
						assert.Equal(t, testIdentity, tx.CallOrderAt(0))
						return nil
					})
				})
				t.Run("Does not change length or other indices", func(t *testing.T) {
					firstIdentity := moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "test.go", Line: 4}}))
					secondIdentity := moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "test.go", Line: 5}}))
					updatedIdentity := moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "test.go", Line: 6}}))
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
				firstIdentity := moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "test.go", Line: 7}}))
				secondIdentity := moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "test.go", Line: 8}}))
				thirdIdentity := moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "test.go", Line: 9}}))
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
		})
		testMoment := moment.NewMoment(
			moment.NewFn[struct{}, struct{}](func(ctx context.Context, args struct{}) (struct{}, error) {
				return struct{}{}, nil
			}),
			struct{}{},
		)
		t.Run("Memo Table", func(t *testing.T) {
			ephemeralTransactTest(t, func(ctx context.Context, tx executiontype.Container) error {
				assert.False(t, tx.HasMoment(testIdentity))
				m, ok := tx.GetMoment(testIdentity)
				assert.False(t, ok)
				assert.Zero(t, m)
				tx.SetMoment(testIdentity, *testMoment)
				return nil
			}, func(ctx context.Context, tx executiontype.Container) error {
				assert.True(t, tx.HasMoment(testIdentity))
				m, ok := tx.GetMoment(testIdentity)
				assert.True(t, ok)
				assert.Equal(t, *testMoment, m)

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
				otherIdentity := moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "test.go", Line: 10}}))
				ephemeralTransactTest(t, func(ctx context.Context, tx executiontype.Container) error {
					tx.SetMoment(testIdentity, *testMoment)
					return nil
				}, func(ctx context.Context, tx executiontype.Container) error {
					assert.False(t, tx.HasMoment(otherIdentity))
					return nil
				})
			})
			t.Run("Delete is idempotent", func(t *testing.T) {
				otherIdentity := moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "test.go", Line: 11}}))
				ephemeralTransactTest(t, func(ctx context.Context, tx executiontype.Container) error {
					tx.SetMoment(testIdentity, *testMoment)
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
				secondIdentity := moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "test.go", Line: 3}}))
				ephemeralTransactTest(t, func(ctx context.Context, tx executiontype.Container) error {
					// Initially empty
					count := 0
					for range tx.KnownMoments() {
						count++
					}
					assert.Equal(t, 0, count)

					// Add some moments
					tx.SetMoment(testIdentity, *testMoment)
					tx.SetMoment(secondIdentity, *testMoment)
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
						srcMoments.Add(moment.NewIdentity(t.Context(), moment.Callpath([]moment.Callsite{{File: "test.go", Line: i}})))
					}
					ephemeralTransactTest(t, func(ctx context.Context, tx executiontype.Container) error {
						for m := range srcMoments.Iter() {
							tx.SetMoment(m, *testMoment)
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
					tx.SetMoment(testIdentity, *testMoment)
					tx.SetMoment(testIdentity, *testMoment)
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
			testutil.WithEphemeralDBRoot(t, func(db util.DbRoot) {
				firstContainer := fdbexec.NewContainer(task.NewId(), db)
				secondContainer := fdbexec.NewContainer(task.NewId(), db)

				err := firstContainer.Transact(t.Context(), func(ctx context.Context, tx executiontype.Container) error {
					tx.SetMoment(testIdentity, *testMoment)
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
