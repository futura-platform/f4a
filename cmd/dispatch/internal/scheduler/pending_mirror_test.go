package scheduler

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/futura-platform/f4a/internal/reliableset"
	"github.com/futura-platform/f4a/internal/task"
)

func TestPendingMirror(t *testing.T) {
	add := func(id task.Id) reliableset.TLogEntry[task.Id] {
		return reliableset.TLogEntry[task.Id]{Op: reliableset.LogOperationAdd, Value: id}
	}
	remove := func(id task.Id) reliableset.TLogEntry[task.Id] {
		return reliableset.TLogEntry[task.Id]{Op: reliableset.LogOperationRemove, Value: id}
	}

	t.Run("a re-queue collapsed out of the stream is still attempted", func(t *testing.T) {
		m := newPendingMirror(taskIDSet("x"))
		// The scheduler assigned x and a draining runner re-queued it inside
		// one chunk: the stream nets that to nothing.
		m.apply(nil)
		require.True(t, m.snapshot().Equal(taskIDSet("x")))
	})

	t.Run("an assignment that did echo back leaves nothing to attempt", func(t *testing.T) {
		m := newPendingMirror(taskIDSet("x"))
		m.apply([]reliableset.TLogEntry[task.Id]{remove("x")})
		require.True(t, m.snapshot().IsEmpty())
	})

	t.Run("membership follows every add and remove in order", func(t *testing.T) {
		m := newPendingMirror(taskIDSet("d"))
		m.apply([]reliableset.TLogEntry[task.Id]{add("c"), remove("c"), remove("d"), add("d"), add("e")})
		require.True(t, m.snapshot().Equal(taskIDSet("d", "e")))
	})

	t.Run("batches coalesce into one wake-up and a snapshot is a copy", func(t *testing.T) {
		m := newPendingMirror(taskIDSet())
		m.apply([]reliableset.TLogEntry[task.Id]{add("a")})
		m.apply([]reliableset.TLogEntry[task.Id]{add("b")})
		<-m.dirty
		select {
		case <-m.dirty:
			t.Fatal("expected the second batch to coalesce into the first wake-up")
		default:
		}
		s := m.snapshot()
		s.Add("z")
		require.True(t, m.snapshot().Equal(taskIDSet("a", "b")))
	})
}
