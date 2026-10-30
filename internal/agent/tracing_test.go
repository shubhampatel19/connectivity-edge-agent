package agent

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/shubhampatel19/connectivity-contracts/wire"
	"github.com/shubhampatel19/connectivity-go-kit/platform"
)

func TestRequestsCarryTheTraceToTheCloud(t *testing.T) {
	spans := tracetest.NewSpanRecorder()
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTracerProvider(prevTP); otel.SetTextMapPropagator(prevProp) })

	var traceparent string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceparent = r.Header.Get("traceparent")
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "key", time.Second)
	ctx, parent := platform.Tracer().Start(context.Background(), "send telemetry")
	err := c.SendTelemetry(ctx, "dev-1", "token", wire.TelemetryBatch{BatchID: "b1", DeviceID: "dev-1"})
	parent.End()
	if err == nil {
		t.Fatal("a 503 should be an error")
	}

	ended := spans.Ended()
	if len(ended) != 2 {
		t.Fatalf("want the request span and its parent, got %d spans", len(ended))
	}
	req := ended[0]
	if req.Name() != "POST /v1/telemetry" || req.Parent().SpanID() != parent.SpanContext().SpanID() {
		t.Errorf("request span %q is not a child of the send span", req.Name())
	}
	if req.Status().Code != codes.Error {
		t.Errorf("a failed request should mark its span, got %v", req.Status())
	}
	want := "00-" + req.SpanContext().TraceID().String() + "-" + req.SpanContext().SpanID().String() + "-01"
	if traceparent != want {
		t.Errorf("cloud saw traceparent %q, want %q", traceparent, want)
	}
}
