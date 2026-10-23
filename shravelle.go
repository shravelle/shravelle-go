// Package shravelle is the Shravelle APM SDK for Go. It configures
// OpenTelemetry tracing, metrics and logs with OTLP/HTTP exporters pointed
// at the Shravelle ingest gateway and adds helpers for errors, users, custom
// events and deployment markers.
//
//	shutdown, err := shravelle.Start(ctx, shravelle.Config{ServiceName: "checkout"})
//	defer shutdown(context.Background())
//	http.Handle("/", shravelle.HTTPHandler(mux, "server"))
//
// Go has no runtime auto-instrumentation, so wrap entry points with the
// contrib middleware: net/http (HTTPHandler / HTTPClient), gRPC, Gin, Echo,
// database/sql — see sdk/go/README.md.
package shravelle

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Config configures the SDK. Empty fields fall back to SHRAVELLE_* env vars.
type Config struct {
	Endpoint       string  // SHRAVELLE_ENDPOINT, default http://localhost:4318
	APIKey         string  // SHRAVELLE_API_KEY
	ServiceName    string  // SHRAVELLE_SERVICE_NAME
	ServiceVersion string  // SHRAVELLE_SERVICE_VERSION
	Environment    string  // SHRAVELLE_ENV, default production
	SampleRate     float64 // 0 means SHRAVELLE_SAMPLE_RATE or 1
	MetricInterval time.Duration
	Attributes     map[string]string

	// Continuous profiling (CPU, heap, goroutines); on unless disabled here
	// or with SHRAVELLE_PROFILING=false. See profiler.go.
	DisableProfiling   bool
	ProfileInterval    time.Duration // default 60s
	ProfileCPUDuration time.Duration // default 10s
	// DisableRuntimeMetrics turns off CPU/memory/GC/goroutine metrics.
	DisableRuntimeMetrics bool
}

var current Config

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// Start installs global tracer and meter providers. Call the returned
// function on shutdown to flush buffered telemetry.
func Start(ctx context.Context, c Config) (func(context.Context) error, error) {
	if c.Endpoint == "" {
		c.Endpoint = env("SHRAVELLE_ENDPOINT", "http://localhost:4318")
	}
	c.Endpoint = strings.TrimRight(c.Endpoint, "/")
	if c.APIKey == "" {
		c.APIKey = os.Getenv("SHRAVELLE_API_KEY")
	}
	if c.ServiceName == "" {
		c.ServiceName = env("SHRAVELLE_SERVICE_NAME", env("OTEL_SERVICE_NAME", "go-app"))
	}
	if c.ServiceVersion == "" {
		c.ServiceVersion = os.Getenv("SHRAVELLE_SERVICE_VERSION")
	}
	if c.Environment == "" {
		c.Environment = env("SHRAVELLE_ENV", "production")
	}
	if c.SampleRate == 0 {
		c.SampleRate, _ = strconv.ParseFloat(env("SHRAVELLE_SAMPLE_RATE", "1"), 64)
	}
	if c.MetricInterval == 0 {
		c.MetricInterval = 30 * time.Second
	}
	if c.APIKey == "" {
		return nil, errors.New("shravelle: APIKey / SHRAVELLE_API_KEY is required")
	}
	if os.Getenv("SHRAVELLE_PROFILING") == "false" {
		c.DisableProfiling = true
	}
	if c.ProfileInterval <= 0 {
		c.ProfileInterval = time.Minute
	}
	if c.ProfileCPUDuration <= 0 || c.ProfileCPUDuration > c.ProfileInterval {
		c.ProfileCPUDuration = min(10*time.Second, c.ProfileInterval)
	}
	current = c

	host, _ := os.Hostname()
	attrs := []attribute.KeyValue{
		semconv.ServiceName(c.ServiceName), semconv.ServiceVersion(c.ServiceVersion),
		attribute.String("deployment.environment.name", c.Environment), semconv.HostName(host),
		semconv.ProcessRuntimeName("go"), semconv.ProcessRuntimeVersion(runtime.Version()),
		attribute.String("telemetry.distro.name", "shravelle-go"),
	}
	for k, v := range c.Attributes {
		attrs = append(attrs, attribute.String(k, v))
	}
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(semconv.SchemaURL, attrs...))
	if err != nil {
		return nil, err
	}
	headers := map[string]string{"x-api-key": c.APIKey}
	insecure := strings.HasPrefix(c.Endpoint, "http://")
	hostPort := strings.TrimPrefix(strings.TrimPrefix(c.Endpoint, "https://"), "http://")

	topts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(hostPort), otlptracehttp.WithHeaders(headers), otlptracehttp.WithCompression(otlptracehttp.GzipCompression)}
	mopts := []otlpmetrichttp.Option{otlpmetrichttp.WithEndpoint(hostPort), otlpmetrichttp.WithHeaders(headers), otlpmetrichttp.WithCompression(otlpmetrichttp.GzipCompression)}
	if insecure {
		topts = append(topts, otlptracehttp.WithInsecure())
		mopts = append(mopts, otlpmetrichttp.WithInsecure())
	}
	texp, err := otlptracehttp.New(ctx, topts...)
	if err != nil {
		return nil, err
	}
	mexp, err := otlpmetrichttp.New(ctx, mopts...)
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(texp),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(c.SampleRate))),
	)
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(mexp, sdkmetric.WithInterval(c.MetricInterval))))
	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	if !c.DisableRuntimeMetrics {
		if err := registerRuntimeMetrics(mp.Meter("shravelle/runtime")); err != nil {
			return nil, err
		}
	}
	stopProfiler := func() {}
	if !c.DisableProfiling {
		stopProfiler = startProfiler(c)
	}
	return func(ctx context.Context) error {
		stopProfiler()
		return errors.Join(tp.Shutdown(ctx), mp.Shutdown(ctx))
	}, nil
}

// HTTPHandler instruments an http.Handler (each request becomes a
// transaction). With Go's ServeMux the transaction is named after the
// matched pattern ("GET /orders/{id}"); otherwise the Shravelle backend
// derives a low-cardinality name from the path.
func HTTPHandler(h http.Handler, operation string) http.Handler {
	named := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.ServeHTTP(w, r)
		if r.Pattern != "" { // set by ServeMux while routing (Go 1.23+)
			route := r.Pattern
			if i := strings.IndexByte(route, ' '); i >= 0 {
				route = route[i+1:]
			}
			span := trace.SpanFromContext(r.Context())
			span.SetName(r.Method + " " + route)
			span.SetAttributes(attribute.String("http.route", route))
		}
	})
	return otelhttp.NewHandler(named, operation, otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string {
		return r.Method // never the raw path: unbounded cardinality
	}))
}

// HTTPClient returns an instrumented client: outgoing calls become external
// segments and propagate the trace (traceparent header).
func HTTPClient() *http.Client {
	return &http.Client{Transport: otelhttp.NewTransport(http.DefaultTransport)}
}

// Tracer returns the Shravelle tracer.
func Tracer() trace.Tracer { return otel.Tracer("shravelle") }

// Meter returns the Shravelle meter for custom metrics.
func Meter() metric.Meter { return otel.Meter("shravelle") }

// Segment starts a custom span: ctx, end := shravelle.Segment(ctx, "pricing"); defer end()
func Segment(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, func()) {
	ctx, span := Tracer().Start(ctx, name, trace.WithAttributes(attrs...))
	return ctx, func() { span.End() }
}

// NoticeError records err on the current span and marks it failed.
func NoticeError(ctx context.Context, err error, attrs ...attribute.KeyValue) {
	if err == nil {
		return
	}
	span := trace.SpanFromContext(ctx)
	span.RecordError(err, trace.WithStackTrace(true), trace.WithAttributes(attrs...))
	span.SetStatus(codes.Error, err.Error())
}

// Recover reports a panic as an error on the current span and re-panics.
// Use as: defer shravelle.Recover(ctx)
func Recover(ctx context.Context) {
	if r := recover(); r != nil {
		NoticeError(ctx, fmt.Errorf("panic: %v", r), attribute.Bool("exception.escaped", true))
		panic(r)
	}
}

// SetUser tags the current transaction with the end user.
func SetUser(ctx context.Context, id string) {
	trace.SpanFromContext(ctx).SetAttributes(attribute.String("enduser.id", id))
}

// AddAttributes adds custom attributes to the current transaction.
func AddAttributes(ctx context.Context, attrs ...attribute.KeyValue) {
	trace.SpanFromContext(ctx).SetAttributes(attrs...)
}

// RecordEvent records a business event as a span event on the current span.
func RecordEvent(ctx context.Context, name string, attrs ...attribute.KeyValue) {
	trace.SpanFromContext(ctx).AddEvent(name, trace.WithAttributes(attrs...))
}

// RecordDeployment sends a deployment marker for the configured service.
func RecordDeployment(ctx context.Context, version, commit, description string) error {
	c := current
	if c.Endpoint == "" {
		c.Endpoint = strings.TrimRight(env("SHRAVELLE_ENDPOINT", "http://localhost:4318"), "/")
		c.APIKey = os.Getenv("SHRAVELLE_API_KEY")
		c.ServiceName = env("SHRAVELLE_SERVICE_NAME", "go-app")
	}
	body, _ := json.Marshal(map[string]string{"service": c.ServiceName, "version": version, "commit": commit, "description": description, "user": os.Getenv("USER")})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Endpoint+"/v1/deployments", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-API-Key", c.APIKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("shravelle: deployment marker failed: HTTP %d", resp.StatusCode)
	}
	return nil
}
