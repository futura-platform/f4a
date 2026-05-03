package serverutil

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/trace"
)

func TestProviderUnsetHelpers(t *testing.T) {
	ctx := t.Context()

	require.True(t, tracerProviderUnset(otel.GetTracerProvider()))
	require.True(t, meterProviderUnset(otel.GetMeterProvider()))
	require.True(t, loggerProviderUnset(global.GetLoggerProvider()))
	require.True(t, textMapPropagatorUnset(otel.GetTextMapPropagator()))

	tp := trace.NewTracerProvider()
	t.Cleanup(func() { _ = tp.Shutdown(ctx) })
	mp := metric.NewMeterProvider()
	t.Cleanup(func() { _ = mp.Shutdown(ctx) })
	lp := log.NewLoggerProvider()
	t.Cleanup(func() { _ = lp.Shutdown(ctx) })

	t.Run("tracer provider", func(t *testing.T) {
		require.True(t, tracerProviderUnset(otel.GetTracerProvider()))
		otel.SetTracerProvider(tp)
		require.False(t, tracerProviderUnset(otel.GetTracerProvider()))
	})

	t.Run("meter provider", func(t *testing.T) {
		require.True(t, meterProviderUnset(otel.GetMeterProvider()))
		otel.SetMeterProvider(mp)
		require.False(t, meterProviderUnset(otel.GetMeterProvider()))
	})

	t.Run("logger provider", func(t *testing.T) {
		require.True(t, loggerProviderUnset(global.GetLoggerProvider()))
		global.SetLoggerProvider(lp)
		require.False(t, loggerProviderUnset(global.GetLoggerProvider()))
	})

	t.Run("text map propagator", func(t *testing.T) {
		require.True(t, textMapPropagatorUnset(otel.GetTextMapPropagator()))
		otel.SetTextMapPropagator(propagation.TraceContext{})
		require.False(t, textMapPropagatorUnset(otel.GetTextMapPropagator()))
	})
}
