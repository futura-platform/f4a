package task

import (
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
	dbutil "github.com/futura-platform/f4a/internal/util/db"
	"go.opentelemetry.io/otel/trace"
)

// spanContextSerializer encodes the minimal fields needed to reconstruct a
// remote span context for a span link: trace id (16 bytes), span id (8 bytes),
// and trace flags (1 byte, carries the sampled bit).
type spanContextSerializer struct{}

const spanContextEncodedLength = 16 + 8 + 1

// Marshal implements dbutil.Serializer.
func (spanContextSerializer) Marshal(sc trace.SpanContext) []byte {
	if !sc.IsValid() {
		return nil
	}
	out := make([]byte, 0, spanContextEncodedLength)
	traceId := sc.TraceID()
	spanId := sc.SpanID()
	out = append(out, traceId[:]...)
	out = append(out, spanId[:]...)
	out = append(out, byte(sc.TraceFlags()))
	return out
}

// Unmarshal implements dbutil.Serializer.
func (spanContextSerializer) Unmarshal(bytes []byte) (trace.SpanContext, error) {
	if len(bytes) == 0 {
		return trace.SpanContext{}, nil
	}
	if len(bytes) != spanContextEncodedLength {
		return trace.SpanContext{}, fmt.Errorf("invalid span context encoding: expected %d bytes, got %d", spanContextEncodedLength, len(bytes))
	}
	var traceId trace.TraceID
	var spanId trace.SpanID
	copy(traceId[:], bytes[:16])
	copy(spanId[:], bytes[16:24])
	return trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceId,
		SpanID:     spanId,
		TraceFlags: trace.TraceFlags(bytes[24]),
		Remote:     true,
	}), nil
}

// LastRunSpan stores the span context of the task's most recent "run" root
// span. Each run attempt atomically swaps its own span context in and links
// back to the previous value, chaining the task's runs across machines.
func (k TaskKey) LastRunSpan() dbutil.TypedKey[trace.SpanContext] {
	return dbutil.NewTypedKey(
		k.d.Pack(tuple.Tuple{string(k.id), "last_run_span"}),
		spanContextSerializer{},
	)
}
