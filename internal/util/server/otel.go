package serverutil

import (
	"context"
	"fmt"
	"net"
	"os"
	"reflect"
	"sync/atomic"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/global"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
)

// isOTelDelegatingGlobalProvider reports whether v is still the concrete type
// OpenTelemetry uses as a package-global placeholder before the first
// SetTracerProvider / SetMeterProvider / SetLoggerProvider / SetTextMapPropagator
// call. There is no official/supported way to ask whether globals have been
// configured; they are never nil and are not the noop types, so we match these
// internal implementations by package path and struct name.
func isOTelDelegatingGlobalProvider(v any, pkgPath, typeName string) bool {
	t := reflect.TypeOf(v)
	if t == nil {
		return false
	}
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return t.Kind() == reflect.Struct && t.PkgPath() == pkgPath && t.Name() == typeName
}

// tracerProviderUnset reports whether tp is still the global delegating tracer
// provider (i.e. otel.SetTracerProvider has not yet replaced it).
func tracerProviderUnset(tp oteltrace.TracerProvider) bool {
	return isOTelDelegatingGlobalProvider(tp, "go.opentelemetry.io/otel/internal/global", "tracerProvider")
}

// meterProviderUnset reports whether mp is still the global delegating meter
// provider (i.e. otel.SetMeterProvider has not yet replaced it).
func meterProviderUnset(mp otelmetric.MeterProvider) bool {
	return isOTelDelegatingGlobalProvider(mp, "go.opentelemetry.io/otel/internal/global", "meterProvider")
}

// loggerProviderUnset reports whether lp is still the global delegating logger
// provider (i.e. global.SetLoggerProvider has not yet replaced it).
func loggerProviderUnset(lp otellog.LoggerProvider) bool {
	return isOTelDelegatingGlobalProvider(lp, "go.opentelemetry.io/otel/log/internal/global", "loggerProvider")
}

// textMapPropagatorUnset reports whether p is still the global delegating
// TextMapPropagator (i.e. otel.SetTextMapPropagator has not yet replaced it).
func textMapPropagatorUnset(p propagation.TextMapPropagator) bool {
	return isOTelDelegatingGlobalProvider(p, "go.opentelemetry.io/otel/internal/global", "textMapPropagator")
}

var hasBootstrappedOTEL atomic.Bool

// BootstrapOTEL bootstraps the OTEL SDK and returns a function to clean up the OTEL SDK.
// The function will only bootstrap OTEL providers if they have not been bootstrapped yet.
func BootstrapOTEL(ctx context.Context) (close func(ctx context.Context), err error) {
	if testing.Testing() {
		return func(ctx context.Context) {}, nil
	}

	if !hasBootstrappedOTEL.CompareAndSwap(false, true) {
		panic("OTEL already bootstrapped (this should never happen)")
	}

	var mp *metric.MeterProvider
	var tp *trace.TracerProvider
	var lp *log.LoggerProvider
	cleanup := func(ctx context.Context) {
		if tp != nil {
			_ = tp.Shutdown(ctx)
		}
		if mp != nil {
			_ = mp.Shutdown(ctx)
		}
		if lp != nil {
			_ = lp.Shutdown(ctx)
		}
		hasBootstrappedOTEL.Store(false)
	}
	defer func() {
		if err != nil {
			cleanup(ctx)
		}
	}()

	if textMapPropagatorUnset(otel.GetTextMapPropagator()) {
		otel.SetTextMapPropagator(propagation.TraceContext{})
	}

	dialer := &net.Dialer{}
	dialNetwork := os.Getenv("OTEL_EXPORTER_OTLP_DIAL_NETWORK")
	if dialNetwork == "" {
		dialNetwork = "tcp"
	}
	dialOption := grpc.WithContextDialer(func(ctx context.Context, s string) (net.Conn, error) {
		return dialer.DialContext(ctx, dialNetwork, s)
	})
	if tracerProviderUnset(otel.GetTracerProvider()) {
		traceExporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithDialOption(dialOption))
		if err != nil {
			return nil, fmt.Errorf("failed to create trace exporter: %w", err)
		}
		tp = trace.NewTracerProvider(trace.WithBatcher(traceExporter))
		otel.SetTracerProvider(tp)
	}

	if meterProviderUnset(otel.GetMeterProvider()) {
		meterExporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithDialOption(dialOption))
		if err != nil {
			return nil, fmt.Errorf("failed to create meter exporter: %w", err)
		}
		meterReader := metric.NewPeriodicReader(meterExporter)
		mp = metric.NewMeterProvider(metric.WithReader(meterReader))
		otel.SetMeterProvider(mp)
	}

	if loggerProviderUnset(global.GetLoggerProvider()) {
		logExporter, err := otlploggrpc.New(ctx, otlploggrpc.WithDialOption(dialOption))
		if err != nil {
			return nil, fmt.Errorf("failed to create log exporter: %w", err)
		}
		lp = log.NewLoggerProvider(log.WithProcessor(log.NewBatchProcessor(logExporter)))
		global.SetLoggerProvider(lp)
	}

	return cleanup, nil
}
