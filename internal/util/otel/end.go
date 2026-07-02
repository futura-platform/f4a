// Package otelutil holds small OpenTelemetry helpers shared across f4a.
package otelutil

import (
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// End completes the span, recording err as an exception event and setting the
// span status to Error when err is non-nil. The OTel Go API deliberately keeps
// RecordError and SetStatus separate; for spans that track a whole operation,
// both must be called for the span to read as failed in trace viewers.
//
// CONVENTION: every function that opens a span covering its entire body should
// use a named error return and defer this helper immediately after Start:
//
//	func doThing(ctx context.Context) (err error) {
//		ctx, span := tracer.Start(ctx, "doThing")
//		defer func() { otelutil.End(span, err) }()
//		// every `return err` path is now recorded automatically
//	}
//
// PITFALL: `defer otelutil.End(span, err)` (without the closure) is WRONG —
// deferred call arguments are evaluated immediately, so err would always be
// nil. The closure is required to observe the function's final return value.
//
// Errors that are handled rather than returned should not go through this
// helper; a span whose operation ultimately succeeded should not carry Error
// status.
func End(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}
