## Per file checking

Whenever you edit a go file, run the following command to check for any issues:

`gopls check -severity=hint <filename>`

## OTel span error convention

Any function that opens a span covering its entire body must use a named error
return and defer `otelutil.End` (from `internal/util/otel`) immediately after
`tracer.Start`, instead of `defer span.End()` and manual
`RecordError`/`SetStatus` calls:

```go
func doThing(ctx context.Context) (err error) {
	ctx, span := tracer.Start(ctx, "doThing")
	defer func() { otelutil.End(span, err) }()
	// ...
}
```

The closure is required: `defer otelutil.End(span, err)` evaluates `err`
immediately and would always record nil. Do not route handled (non-returned)
errors through this helper, and leave spans that don't track a fallible
operation (e.g. zero-duration marker spans) on plain `span.End()`.

For spans that end mid-function rather than at return (e.g. per-iteration
spans in a loop), call `otelutil.End(span, err)` directly at the end point
instead of the manual `RecordError`/`SetStatus`/`End` sequence.
