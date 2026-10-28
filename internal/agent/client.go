package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/spr732/connectivity-contracts/wire"
)

var ErrUnauthorized = errors.New("unauthorized")

// StatusError is a non-2xx response from the backend.
type StatusError struct {
	Code int
	Body string
}

func (e *StatusError) Error() string { return fmt.Sprintf("backend returned %d: %s", e.Code, e.Body) }

// Client talks to ingestion (directly now; through the site gateway in a later
// phase). One Client is shared by every simulated device in the process, the
// same way a real gateway multiplexes many devices onto one upstream.
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
	err := c.post(ctx, "/v1/register", map[string]string{"X-Enrollment-Key": c.enrollmentKey}, req, &resp)
	return resp.Token, err
}

func (c *Client) SendTelemetry(ctx context.Context, deviceID, token string, b wire.TelemetryBatch) error {
	return c.post(ctx, "/v1/telemetry", deviceHeaders(deviceID, token), b, nil)
}

func (c *Client) Heartbeat(ctx context.Context, deviceID, token string) error {
	return c.post(ctx, "/v1/heartbeat", deviceHeaders(deviceID, token), struct{}{}, nil)
}

func deviceHeaders(deviceID, token string) map[string]string {
	return map[string]string{"X-Device-ID": deviceID, "Authorization": "Bearer " + token}
}

func (c *Client) post(ctx context.Context, path string, headers map[string]string, body, out any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+path, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

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

// isPermanent reports whether retrying the same request can never succeed.
// Retrying those forever would wedge the device's send queue behind one bad
// batch.
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
