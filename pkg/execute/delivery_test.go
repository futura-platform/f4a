package execute

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/futura-platform/futura"
	"github.com/futura-platform/futura/ftype/executiontype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// compressDeliverySchedule shrinks the delivery retry policy so exhaustion
// tests run in milliseconds. Tests using it must not run in parallel.
func compressDeliverySchedule(t *testing.T, attempts uint64, base time.Duration) {
	t.Helper()
	prevAttempts, prevBase := maxDeliveryAttempts, deliveryBackoffBase
	maxDeliveryAttempts, deliveryBackoffBase = attempts, base
	t.Cleanup(func() {
		maxDeliveryAttempts, deliveryBackoffBase = prevAttempts, prevBase
	})
}

// ephemeralHTTPServer mirrors testutil.NewEphemeralHTTPServer, which cannot
// be imported here (testutil imports this package for its executor mock).
func ephemeralHTTPServer(t testing.TB, handler func(http.ResponseWriter, *http.Request)) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(handler))
	t.Cleanup(server.Close)
	return server
}

func drainBackoff(b backoff.BackOff) []time.Duration {
	var intervals []time.Duration
	for {
		next := b.NextBackOff()
		if next == backoff.Stop {
			return intervals
		}
		intervals = append(intervals, next)
	}
}

func TestNewDeliveryBackoff(t *testing.T) {
	compressDeliverySchedule(t, 5, time.Millisecond)

	t.Run("fresh completion gets the full schedule", func(t *testing.T) {
		intervals := drainBackoff(newDeliveryBackoff(time.Now()))
		assert.Equal(t, []time.Duration{
			1 * time.Millisecond,
			2 * time.Millisecond,
			4 * time.Millisecond,
			8 * time.Millisecond,
		}, intervals)
	})

	t.Run("partially elapsed schedule resumes instead of restarting", func(t *testing.T) {
		// attempts are scheduled at completedAt + base*(2^i - 1) = 0,1,3,7,15ms;
		// 3ms elapsed means attempts 0..2 are considered spent
		intervals := drainBackoff(newDeliveryBackoff(time.Now().Add(-3 * time.Millisecond)))
		assert.Len(t, intervals, 2)
	})

	t.Run("fully elapsed schedule yields no retries", func(t *testing.T) {
		intervals := drainBackoff(newDeliveryBackoff(time.Now().Add(-time.Hour)))
		assert.Empty(t, intervals)
	})
}

func TestResultKey(t *testing.T) {
	t.Run("distinct pairs with identical concatenation get distinct keys", func(t *testing.T) {
		assert.NotEqual(t,
			resultKey([]byte("ab"), []byte("c")),
			resultKey([]byte("a"), []byte("bc")),
		)
	})

	t.Run("same pair gets the same key", func(t *testing.T) {
		assert.Equal(t,
			resultKey([]byte("in"), []byte("out")),
			resultKey([]byte("in"), []byte("out")),
		)
	})

	t.Run("never writes into the caller's spare capacity", func(t *testing.T) {
		backing := make([]byte, 8)
		input := backing[:2]
		copy(input, "ab")

		resultKey(input, []byte("scribble"))

		assert.Equal(t, []byte{0, 0, 0, 0, 0, 0}, backing[2:],
			"resultKey must not append into the input's backing array")
	})
}

// Ported from the retired taskManager.postResult tests: the transport
// behavior of a single delivery attempt.
func TestAttemptDelivery(t *testing.T) {
	request := func(u string, failure string) deliveryRequest {
		parsed, err := url.Parse(u)
		require.NoError(t, err)
		return deliveryRequest{
			completedAt: time.Now(),
			callbackUrl: *parsed,
			result:      "the-output",
			failure:     failure,
		}
	}

	t.Run("posts raw output when no error", func(t *testing.T) {
		var body atomic.Pointer[string]
		var contentType atomic.Pointer[string]
		server := ephemeralHTTPServer(t, func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			s, ct := string(b), r.Header.Get("Content-Type")
			body.Store(&s)
			contentType.Store(&ct)
			w.WriteHeader(http.StatusOK)
		})

		err := attemptDelivery(t.Context(), request(server.URL, ""))

		assert.NoError(t, err)
		assert.Equal(t, "the-output", *body.Load())
		assert.Equal(t, "application/octet-stream", *contentType.Load())
	})

	t.Run("posts problem JSON when the flow failed", func(t *testing.T) {
		var body atomic.Pointer[string]
		var contentType atomic.Pointer[string]
		server := ephemeralHTTPServer(t, func(w http.ResponseWriter, r *http.Request) {
			b, _ := io.ReadAll(r.Body)
			s, ct := string(b), r.Header.Get("Content-Type")
			body.Store(&s)
			contentType.Store(&ct)
			w.WriteHeader(http.StatusAccepted)
		})

		err := attemptDelivery(t.Context(), request(server.URL, "task exploded"))

		assert.NoError(t, err)
		assert.Contains(t, *body.Load(), "task exploded")
		assert.Contains(t, *contentType.Load(), "application/problem+json")
	})

	t.Run("returns error on non-accepted status", func(t *testing.T) {
		server := ephemeralHTTPServer(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		})

		err := attemptDelivery(t.Context(), request(server.URL, ""))
		assert.ErrorContains(t, err, "bad status")
	})

	t.Run("returns error when the endpoint is unreachable", func(t *testing.T) {
		err := attemptDelivery(t.Context(), request("http://127.0.0.1:1", ""))
		assert.ErrorContains(t, err, "failed to send result")
	})
}

// settleHarness wires a real executor + in-memory settlement containers to an
// ephemeral callback endpoint whose behavior is switchable per test.
type settleHarness struct {
	executable    Executable
	containers    SettlementContainers
	callbackURL   *url.URL
	posts         atomic.Int64
	userSteps     atomic.Int64
	respondStatus atomic.Int64 // http status to return
	lastBody      atomic.Pointer[string]
	delivered     chan string
}

func newSettleHarness(t *testing.T) *settleHarness {
	t.Helper()
	h := &settleHarness{delivered: make(chan string, 16)}
	h.respondStatus.Store(http.StatusAccepted)

	server := ephemeralHTTPServer(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		s := string(b)
		h.lastBody.Store(&s)
		h.posts.Add(1)
		status := int(h.respondStatus.Load())
		w.WriteHeader(status)
		if status == http.StatusAccepted || status == http.StatusOK {
			select {
			case h.delivered <- s:
			default:
			}
		}
	})

	executor := NewExecutor(func(b futura.FlowBuilder, input string) (string, error) {
		// count STEP executions, not flow-fn invocations: replays re-enter the
		// fn but must not re-execute memoized steps. The input is the step arg
		// so a changed task input invalidates the moment and re-executes.
		if err := futura.Effect(b, func(ctx context.Context, _ string) error {
			h.userSteps.Add(1)
			return nil
		}, input); err != nil {
			return "", err
		}
		return strings.ToUpper(input), nil
	}, NewJsonMarshaller[string, string]())

	h.containers = SettlementContainers{
		User:      executiontype.NewInMemoryContainer(),
		Discharge: executiontype.NewInMemoryContainer(),
	}
	h.executable = executor.ExecuteFrom(h.containers)

	parsed, err := url.Parse(server.URL + "/callback")
	require.NoError(t, err)
	h.callbackURL = parsed
	return h
}

func TestSettleRetriesDeliveryUntilAccepted(t *testing.T) {
	compressDeliverySchedule(t, 5, time.Millisecond)
	h := newSettleHarness(t)

	// fail the first two attempts, then accept
	h.respondStatus.Store(http.StatusInternalServerError)
	go func() {
		for h.posts.Load() < 2 {
			time.Sleep(200 * time.Microsecond)
		}
		h.respondStatus.Store(http.StatusAccepted)
	}()

	err := h.executable.Settle(t.Context(), []byte(`"input"`), h.callbackURL)

	assert.NoError(t, err)
	assert.GreaterOrEqual(t, h.posts.Load(), int64(3))
	assert.Equal(t, int64(1), h.userSteps.Load())
}

// The dead letter queue is not implemented yet: exhausting the delivery
// budget must currently surface its not-implemented panic through the
// discharge flow. Replace the error assertion with real dead-letter
// assertions (park + settle nil) once the queue lands.
func TestSettleParksDeadLetterWhenDeliveryExhausted(t *testing.T) {
	compressDeliverySchedule(t, 2, time.Millisecond)
	h := newSettleHarness(t)

	h.respondStatus.Store(http.StatusInternalServerError)

	err := h.executable.Settle(t.Context(), []byte(`"input"`), h.callbackURL)

	require.Error(t, err)
	assert.ErrorContains(t, err, "failed to discharge result")
	assert.ErrorContains(t, err, "not implemented: dead letter queue")
	assert.GreaterOrEqual(t, h.posts.Load(), int64(2))
}

// Flagship reliability property of the settlement design: an interrupted
// discharge resumes from its durable state — the user flow is not
// re-executed, and delivery picks the schedule back up.
func TestSettleResumesDischargeAfterInterruption(t *testing.T) {
	compressDeliverySchedule(t, 8, 5*time.Millisecond)
	h := newSettleHarness(t)

	// phase 1: endpoint down, cancel mid-retries (worker death analog)
	h.respondStatus.Store(http.StatusInternalServerError)
	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		for h.posts.Load() < 1 {
			time.Sleep(200 * time.Microsecond)
		}
		cancel()
	}()
	err := h.executable.Settle(ctx, []byte(`"input"`), h.callbackURL)
	require.Error(t, err, "interrupted settlement must not report settled")

	// phase 2: endpoint recovers; a fresh Settle on the same containers
	// resumes discharge without re-running the user flow
	h.respondStatus.Store(http.StatusAccepted)
	err = h.executable.Settle(t.Context(), []byte(`"input"`), h.callbackURL)

	assert.NoError(t, err)
	assert.Equal(t, int64(1), h.userSteps.Load(),
		"user flow steps must be memoized across settlement attempts")
	assert.Equal(t, `"INPUT"`, *h.lastBody.Load())
}

// Replaying a settled task must not deliver the result a second time: the
// delivery step is memoized, which is what bounds at-least-once delivery.
func TestSettleDoesNotRedeliverSettledResult(t *testing.T) {
	h := newSettleHarness(t)

	require.NoError(t, h.executable.Settle(t.Context(), []byte(`"input"`), h.callbackURL))
	require.NoError(t, h.executable.Settle(t.Context(), []byte(`"input"`), h.callbackURL))

	assert.Equal(t, int64(1), h.posts.Load(),
		"a replayed settlement must not re-POST an already delivered result")
	assert.Equal(t, int64(1), h.userSteps.Load())
}

// A changed input is a new obligation: the user flow re-runs and the new
// result is delivered with a fresh completedAt (fresh retry budget).
func TestSettleRedeliversForChangedInput(t *testing.T) {
	h := newSettleHarness(t)

	require.NoError(t, h.executable.Settle(t.Context(), []byte(`"first"`), h.callbackURL))
	require.NoError(t, h.executable.Settle(t.Context(), []byte(`"second"`), h.callbackURL))

	assert.Equal(t, int64(2), h.posts.Load())
	assert.Equal(t, `"SECOND"`, *h.lastBody.Load())
	assert.Equal(t, int64(2), h.userSteps.Load(),
		"a new input must re-run the user flow")
}
