package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/shubhampatel19/connectivity-contracts/wire"
	"github.com/shubhampatel19/connectivity-go-kit/platform"
)

var ErrUnauthorized = errors.New("unauthorized")

// StatusError is a non-2xx response from the backend.
type StatusError struct {
	Code int
	Body string
}

func (e *StatusError) Error() string { return fmt.Sprintf("backend returned %d: %s", e.Code, e.Body) }

// Client talks to the ingest API, directly or through the site gateway, which
// serves the same API. One Client is shared by every simulated device in the
// process, the same way a real gateway multiplexes many devices onto one
// upstream.
type Client struct {
	base          string
	enrollmentKey string
	hc            *http.Client
}

func NewClient(baseURL, enrollmentKey string, timeout time.Duration) *Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConns = 512
	tr.MaxIdleConnsPerHost = 256
	return &Client{
		base:          strings.TrimRight(baseURL, "/"),
		enrollmentKey: enrollmentKey,
		hc:            &http.Client{Timeout: timeout, Transport: tr},
	}
}

func (c *Client) Register(ctx context.Context, req wire.RegisterRequest) (string, error) {
	var resp wire.RegisterResponse
	err := c.postJSON(ctx, "/v1/register", "/v1/register", map[string]string{"X-Enrollment-Key": c.enrollmentKey}, req, &resp)
	return resp.Token, err
}

func (c *Client) SendTelemetry(ctx context.Context, deviceID, token string, b wire.TelemetryBatch) error {
	return c.postJSON(ctx, "/v1/telemetry", "/v1/telemetry", deviceHeaders(deviceID, token), b, nil)
}

func (c *Client) SendAlert(ctx context.Context, deviceID, token string, a wire.DeviceAlert) error {
	return c.postJSON(ctx, "/v1/alerts", "/v1/alerts", deviceHeaders(deviceID, token), a, nil)
}

func (c *Client) Heartbeat(ctx context.Context, deviceID, token string) error {
	return c.postJSON(ctx, "/v1/heartbeat", "/v1/heartbeat", deviceHeaders(deviceID, token), struct{}{}, nil)
}

func (c *Client) InitBundle(ctx context.Context, deviceID, token string, req wire.BundleInitRequest) (wire.BundleStatus, error) {
	var st wire.BundleStatus
	err := c.postJSON(ctx, "/v1/bundles", "/v1/bundles", deviceHeaders(deviceID, token), req, &st)
	return st, err
}

func (c *Client) UploadChunk(ctx context.Context, deviceID, token, bundleID string, index int, data []byte) error {
	sum := sha256.Sum256(data)
	h := deviceHeaders(deviceID, token)
	h[wire.HeaderChunkSHA256] = hex.EncodeToString(sum[:])
	path := fmt.Sprintf("/v1/bundles/%s/chunks/%d", bundleID, index)
	return c.do(ctx, http.MethodPut, "/v1/bundles/{bundle_id}/chunks/{index}", path, h, "application/octet-stream", bytes.NewReader(data), nil)
}

func (c *Client) CompleteBundle(ctx context.Context, deviceID, token, bundleID string) (wire.BundleStatus, error) {
	var st wire.BundleStatus
	err := c.postJSON(ctx, "/v1/bundles/{bundle_id}/complete", "/v1/bundles/"+bundleID+"/complete", deviceHeaders(deviceID, token), struct{}{}, &st)
	return st, err
}

func deviceHeaders(deviceID, token string) map[string]string {
	return map[string]string{"X-Device-ID": deviceID, "Authorization": "Bearer " + token}
}

func (c *Client) postJSON(ctx context.Context, route, path string, headers map[string]string, body, out any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	return c.do(ctx, http.MethodPost, route, path, headers, "application/json", bytes.NewReader(payload), out)
}

// do sends one request inside a client span named after its route, carrying
// the trace context in a traceparent header so the cloud continues the trace.
func (c *Client) do(ctx context.Context, method, route, path string, headers map[string]string, contentType string, body io.Reader, out any) (err error) {
	ctx, span := platform.Tracer().Start(ctx, method+" "+route,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("http.request.method", method), attribute.String("url.path", path)))
	defer func() {
		platform.RecordError(ctx, err)
		span.End()
	}()

	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", contentType)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	platform.InjectHTTP(ctx, req.Header)

	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	span.SetAttributes(attribute.Int("http.response.status_code", resp.StatusCode))

	if resp.StatusCode == http.StatusUnauthorized {
		_, _ = io.Copy(io.Discard, resp.Body)
		return ErrUnauthorized
	}
	if resp.StatusCode >= 300 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return &StatusError{Code: resp.StatusCode, Body: strings.TrimSpace(string(msg))}
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	// Drain so the keep-alive connection goes back to the pool.
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// isPermanent reports whether retrying the same telemetry request can never
// succeed. Retrying those forever would wedge the device's send queue behind
// one bad batch.
func isPermanent(err error) bool {
	var se *StatusError
	if !errors.As(err, &se) {
		return false
	}
	switch se.Code {
	case http.StatusRequestTimeout, http.StatusTooManyRequests:
		return false
	}
	return se.Code >= 400 && se.Code < 500
}

// errorReason is a low-cardinality label for metrics.
func errorReason(err error) string {
	var se *StatusError
	var ne net.Error
	switch {
	case errors.Is(err, ErrUnauthorized):
		return "unauthorized"
	case errors.As(err, &se) && se.Code >= 500:
		return "http_5xx"
	case errors.As(err, &se):
		return "http_4xx"
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &ne) && ne.Timeout():
		return "timeout"
	default:
		return "network"
	}
}
