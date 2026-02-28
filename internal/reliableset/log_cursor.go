package reliableset

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type logCursor struct {
	set  *Set
	id   string
	hint string
}

func newLogCursor(set *Set) *logCursor {
	hostname, _ := os.Hostname()
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return &logCursor{
			set:  set,
			id:   hex.EncodeToString([]byte(hostname)),
			hint: hostname,
		}
	}
	return &logCursor{
		set:  set,
		id:   hex.EncodeToString(buf),
		hint: hostname,
	}
}

func (c *logCursor) key(kind string) fdb.Key {
	return c.set.cursorKey(c.id, kind)
}

func (c *logCursor) register(tx fdb.Transaction, tail fdb.KeyConvertible) {
	if c.hint != "" {
		tx.Set(c.key(cursorKeyHint), []byte(c.hint))
	}
	c.writeTail(tx, tail)
	c.writeLease(tx, time.Now())
}

func (c *logCursor) writeTail(tx fdb.Transaction, tail fdb.KeyConvertible) {
	if tail == nil {
		panic("cursor tail is nil")
	}
	tx.Set(c.key(cursorKeyTail), tail.FDBKey())
}

func (c *logCursor) writeLease(tx fdb.Transaction, now time.Time) {
	tx.Set(c.key(cursorKeyLease), encodeLease(now.Add(cursorLeaseTTL)))
}

func (c *logCursor) advance(ctx context.Context, tail fdb.KeyConvertible) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := c.set.db.TransactContext(ctx, func(tx fdb.Transaction) (any, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		c.writeTail(tx, tail)
		c.writeLease(tx, time.Now())
		return nil, nil
	})
	return err
}

// leaseLoop refreshes the lease of the cursor periodically.
func (c *logCursor) leaseLoop(ctx context.Context) {
	if cursorLeaseRefresh <= 0 {
		return
	}
	ticker := time.NewTicker(cursorLeaseRefresh)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = c.set.db.Transact(func(tx fdb.Transaction) (any, error) {
				c.writeLease(tx, time.Now())
				return nil, nil
			})
		}
	}
}

func (c *logCursor) clear(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := c.set.db.TransactContext(ctx, func(tx fdb.Transaction) (any, error) {
		tx.Clear(c.key(cursorKeyTail))
		tx.Clear(c.key(cursorKeyLease))
		tx.Clear(c.key(cursorKeyHint))
		return nil, nil
	})
	return err
}

func (c *logCursor) bestEffortClear() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = c.clear(ctx)
}
