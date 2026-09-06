package scheduler

import (
	"sync"

	mapset "github.com/deckarep/golang-set/v2"

	"github.com/futura-platform/f4a/internal/reliableset"
	"github.com/futura-platform/f4a/internal/task"
)

// pendingMirror is the scheduler's copy of the pending set's membership,
// maintained from the set's stream and nothing else.
//
// The stream reports the absolute net change of each chunk it reads. A task
// the scheduler assigned (the Remove it wrote itself) and that a draining
// runner then re-queued (an Add) nets to no event at all when both land in
// one chunk, and chunks span however long the scheduler takes to read the
// next one. A scheduler that only remembers its failures then forgets the
// task: it is pending in FoundationDB and never attempted again, with
// nothing to log. That was the 2026-09-06 100k wedge (11,287 pending, no
// runs, an idle fleet, `pending_ids_count=0` every tick) and the 2026-08
// 688-orphan wedge. Mirroring membership makes every pass attempt exactly
// what the set holds, whatever the chunking collapsed.
//
// The mirror is fed by its own goroutine so it is current while a pass runs:
// a pass at scale takes seconds to minutes, and a mirror only advanced
// between passes plans thousands of already-assigned ids (each one a wasted
// slot in the plan and a skipped assignTask), which stretched a 2,000-task
// re-queue to twelve minutes in the first validation run.
type pendingMirror struct {
	mu    sync.Mutex
	items mapset.Set[task.Id]
	// dirty holds one pending wake-up for the scheduler loop; batches that
	// arrive during a pass coalesce into the next one.
	dirty chan struct{}
}

func newPendingMirror(initial mapset.Set[task.Id]) *pendingMirror {
	return &pendingMirror{items: initial.Clone(), dirty: make(chan struct{}, 1)}
}

// apply folds a stream batch into the mirror and wakes the loop.
func (m *pendingMirror) apply(batch []reliableset.TLogEntry[task.Id]) {
	m.mu.Lock()
	for _, entry := range batch {
		switch entry.Op {
		case reliableset.LogOperationAdd:
			m.items.Add(entry.Value)
		case reliableset.LogOperationRemove:
			m.items.Remove(entry.Value)
		}
	}
	m.mu.Unlock()
	select {
	case m.dirty <- struct{}{}:
	default:
	}
}

// snapshot is what a pass attempts: every task the set holds. A task assigned
// moments ago whose Remove has not echoed back yet is skipped by assignTask
// as no longer pending.
func (m *pendingMirror) snapshot() mapset.Set[task.Id] {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.items.Clone()
}
