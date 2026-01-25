package reliablelock

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/futura-platform/f4a/internal/reliablewatch"
	"github.com/futura-platform/futura/privateencoding"
)

type LeaseValue[T any] struct {
	LeaseExpiration int64
	Holder          T
}

type Lock[T comparable] struct {
	db     fdb.Transactor
	key    fdb.KeyConvertible
	holder T

	leaseDuration   time.Duration
	refreshInterval time.Duration
	now             func() time.Time

	mu              sync.Mutex
	leaseExpiration int64
	refreshCancel   context.CancelFunc
	refreshDone     chan struct{}
}

var (
	ErrNotOwner = errors.New("lock held by another owner")
	ErrNotHeld  = errors.New("lock is not held")
)

const defaultLeaseDuration = 5 * time.Second

type Option func(*options)

type options struct {
	leaseDuration   time.Duration
	refreshInterval time.Duration
	now             func() time.Time
}

func WithLeaseDuration(duration time.Duration) Option {
	return func(o *options) {
		o.leaseDuration = duration
	}
}

func WithRefreshInterval(interval time.Duration) Option {
	return func(o *options) {
		o.refreshInterval = interval
	}
}

func NewLock[T comparable](db fdb.Transactor, key fdb.KeyConvertible, holder T, opts ...Option) *Lock[T] {
	options := options{
		leaseDuration: defaultLeaseDuration,
		now:           time.Now,
	}
	for _, opt := range opts {
		opt(&options)
	}
	if options.leaseDuration <= 0 {
		options.leaseDuration = defaultLeaseDuration
	}
	if options.refreshInterval <= 0 {
		options.refreshInterval = options.leaseDuration / 2
		if options.refreshInterval <= 0 {
			options.refreshInterval = time.Millisecond
		}
	}
	return &Lock[T]{
		db:              db,
		key:             key,
		holder:          holder,
		leaseDuration:   options.leaseDuration,
		refreshInterval: options.refreshInterval,
		now:             options.now,
	}
}

func encodeValue[T comparable](value LeaseValue[T]) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	encoder := privateencoding.NewEncoder[LeaseValue[T]](buf)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeValue[T comparable](raw []byte) (LeaseValue[T], error) {
	decoder := privateencoding.NewDecoder[LeaseValue[T]](bytes.NewReader(raw))
	return decoder.Decode()
}

type acquireResult struct {
	acquired   bool
	expiration int64
}

func (l *Lock[T]) tryAcquire(ctx context.Context, nowUnix int64, held bool, leaseExpiration int64) (bool, int64, error) {
	if err := ctx.Err(); err != nil {
		return false, 0, err
	}
	newExpiration := nowUnix + l.leaseDuration.Nanoseconds()
	encodedValue, err := encodeValue(LeaseValue[T]{
		LeaseExpiration: newExpiration,
		Holder:          l.holder,
	})
	if err != nil {
		return false, 0, err
	}

	result, err := l.db.Transact(func(tx fdb.Transaction) (any, error) {
		if err := ctx.Err(); err != nil {
			return acquireResult{}, err
		}
		currentValue, err := tx.Get(l.key).Get()
		if err != nil {
			return acquireResult{}, err
		}
		if currentValue == nil {
			tx.Set(l.key, encodedValue)
			return acquireResult{acquired: true, expiration: newExpiration}, nil
		}
		currentLease, err := decodeValue[T](currentValue)
		if err != nil {
			return acquireResult{}, err
		}
		if held &&
			currentLease.LeaseExpiration == leaseExpiration &&
			currentLease.LeaseExpiration > nowUnix &&
			currentLease.Holder == l.holder {
			return acquireResult{acquired: true, expiration: currentLease.LeaseExpiration}, nil
		}
		if currentLease.LeaseExpiration <= nowUnix {
			tx.Set(l.key, encodedValue)
			return acquireResult{acquired: true, expiration: newExpiration}, nil
		}
		return acquireResult{acquired: false, expiration: currentLease.LeaseExpiration}, nil
	})
	if err != nil {
		return false, 0, err
	}
	acquire := result.(acquireResult)
	return acquire.acquired, acquire.expiration, nil
}

func (l *Lock[T]) currentLease() (bool, int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.leaseExpiration != 0, l.leaseExpiration
}

func (l *Lock[T]) setLease(expiration int64) {
	l.mu.Lock()
	l.leaseExpiration = expiration
	l.mu.Unlock()
}

func (l *Lock[T]) startRefresh() {
	if l.refreshInterval <= 0 {
		return
	}
	l.stopRefresh()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	l.mu.Lock()
	l.refreshCancel = cancel
	l.refreshDone = done
	l.mu.Unlock()

	go func() {
		defer close(done)

		ticker := time.NewTicker(l.refreshInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := l.refreshLease(ctx); err != nil {
					if errors.Is(err, ErrNotOwner) || errors.Is(err, ErrNotHeld) {
						l.setLease(0)
						return
					}
				}
			}
		}
	}()
}

func (l *Lock[T]) stopRefresh() {
	l.mu.Lock()
	cancel := l.refreshCancel
	done := l.refreshDone
	l.refreshCancel = nil
	l.refreshDone = nil
	l.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}

func (l *Lock[T]) refreshLease(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	nowUnix := l.now().UnixNano()
	_, leaseExpiration := l.currentLease()
	if leaseExpiration == 0 {
		return ErrNotHeld
	}
	newExpiration := nowUnix + l.leaseDuration.Nanoseconds()
	encodedValue, err := encodeValue(LeaseValue[T]{
		LeaseExpiration: newExpiration,
		Holder:          l.holder,
	})
	if err != nil {
		return err
	}
	_, err = l.db.Transact(func(tx fdb.Transaction) (any, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		currentValue, err := tx.Get(l.key).Get()
		if err != nil {
			return nil, err
		}
		if currentValue == nil {
			return nil, ErrNotHeld
		}
		currentLease, err := decodeValue[T](currentValue)
		if err != nil {
			return nil, err
		}
		if currentLease.LeaseExpiration != leaseExpiration || currentLease.LeaseExpiration <= nowUnix || currentLease.Holder != l.holder {
			return nil, ErrNotOwner
		}
		tx.Set(l.key, encodedValue)
		return nil, nil
	})
	if err != nil {
		return err
	}
	l.setLease(newExpiration)
	return nil
}

// Acquire attempts to acquire the lock.
// It will block until the lock is able to be acquired.
func (l *Lock[T]) Acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	held, leaseExpiration := l.currentLease()
	nowUnix := l.now().UnixNano()
	acquired, observedExpiration, err := l.tryAcquire(ctx, nowUnix, held, leaseExpiration)
	if err != nil {
		return err
	}
	if acquired {
		l.setLease(observedExpiration)
		l.startRefresh()
		return nil
	}

	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()

	updates := make(chan LeaseValue[T], 1)
	errCh := make(chan error, 1)
	var updateMu sync.Mutex
	go func() {
		err := reliablewatch.Watch(watchCtx, l.db, l.key, LeaseValue[T]{}, nil, reliablewatch.PrivateEncodingGetter[LeaseValue[T]])(func(value LeaseValue[T]) error {
			updateMu.Lock()
			defer updateMu.Unlock()

			select {
			case updates <- value:
			default:
				select {
				case <-updates:
				default:
				}
				select {
				case updates <- value:
				default:
				}
			}
			return nil
		})
		errCh <- err
	}()

	currentExpiration := observedExpiration
	for {
		wait := time.Until(time.Unix(0, currentExpiration))
		if currentExpiration == 0 || wait < 0 {
			wait = 0
		}
		timer := time.NewTimer(wait)

		stopTimer := func() {
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}

		select {
		case <-ctx.Done():
			stopTimer()
			return ctx.Err()
		case err := <-errCh:
			stopTimer()
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		case updatedValue := <-updates:
			stopTimer()
			currentExpiration = updatedValue.LeaseExpiration
		case <-timer.C:
		}

		nowUnix = l.now().UnixNano()
		if currentExpiration == 0 || currentExpiration <= nowUnix {
			held, leaseExpiration = l.currentLease()
			acquired, observedExpiration, err = l.tryAcquire(ctx, nowUnix, held, leaseExpiration)
			if err != nil {
				return err
			}
			if acquired {
				l.setLease(observedExpiration)
				l.startRefresh()
				return nil
			}
			currentExpiration = observedExpiration
		}
	}
}

// Release releases the lock.
func (l *Lock[T]) Release() error {
	l.stopRefresh()
	_, leaseExpiration := l.currentLease()
	_, err := l.db.Transact(func(tx fdb.Transaction) (any, error) {
		currentValue, err := tx.Get(l.key).Get()
		if err != nil {
			return nil, err
		}
		if currentValue == nil {
			return nil, ErrNotHeld
		}
		currentLease, err := decodeValue[T](currentValue)
		if err != nil {
			return nil, err
		}
		if leaseExpiration == 0 || currentLease.LeaseExpiration != leaseExpiration || currentLease.Holder != l.holder {
			return nil, ErrNotOwner
		}
		tx.Clear(l.key)
		return nil, nil
	})
	if err == nil || errors.Is(err, ErrNotOwner) || errors.Is(err, ErrNotHeld) {
		l.setLease(0)
	}
	return err
}
