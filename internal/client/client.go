// Package client is an HTTP client for the inventory API that retries
// through transient network failures. It's deliberately dial-agnostic: the
// same client works over a real TCP dialer (cmd/api) or a netchaos
// simulated network (internal/client tests), because both satisfy the
// same DialContext signature.
package client

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

// DialContextFunc matches both net.Dialer.DialContext and
// (*netchaos.Network).DialContext, so a chaos-simulated dialer can be
// swapped in without the client knowing about netchaos at all.
type DialContextFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// Client is a small retrying HTTP client.
type Client struct {
	baseURL        string
	httpClient     *http.Client
	maxAttempts    int
	attemptTimeout time.Duration
}

// Option configures a Client.
type Option func(*Client)

// WithDialContext overrides the transport's dialer. Production code leaves
// this unset and gets the stdlib dialer; tests inject a netchaos-backed one.
func WithDialContext(f DialContextFunc) Option {
	return func(c *Client) {
		c.httpClient.Transport.(*http.Transport).DialContext = f
	}
}

// WithMaxAttempts sets the maximum number of attempts per call (default 4).
func WithMaxAttempts(n int) Option {
	return func(c *Client) { c.maxAttempts = n }
}

// WithAttemptTimeout sets the per-attempt deadline (default 500ms).
//
// This is not an optional knob: under packet loss a dropped write reports
// n=len(p), nil to the caller (the request looks sent, but the server
// never receives it and never replies). Without a bounded per-attempt
// deadline the client would simply hang on that attempt forever instead
// of retrying.
func WithAttemptTimeout(d time.Duration) Option {
	return func(c *Client) { c.attemptTimeout = d }
}

// New builds a Client pointed at baseURL (e.g. "http://inventory-api:8080").
func New(baseURL string, opts ...Option) *Client {
	c := &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			// Keep-alives stay on. Closing a connection right after a
			// write races that write's simulated latency: netchaos
			// delivers a delayed write asynchronously, so a Close that
			// follows immediately (as DisableKeepAlives + "Connection:
			// close" would trigger server-side) can tear the connection
			// down before the delayed bytes arrive, surfacing as a bare
			// EOF unrelated to any configured fault. Retries still get a
			// fresh dial: a broken persistConn is evicted from the pool
			// automatically, so the next attempt reconnects on its own.
			Transport: &http.Transport{},
		},
		maxAttempts:    4,
		attemptTimeout: 500 * time.Millisecond,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Result is returned by every call alongside the error, so tests can
// assert on the observed retry behavior instead of guessing at internals.
type Result struct {
	StatusCode int
	Body       []byte
	Attempts   int
}

// maxBackoff caps the exponential backoff between retries.
const maxBackoff = 200 * time.Millisecond

// Do issues method/path (relative to baseURL) with an optional JSON body,
// retrying transport errors and 5xx responses with capped exponential
// backoff. 4xx responses are never retried.
func (c *Client) Do(ctx context.Context, method, path string, body []byte) (Result, error) {
	var lastErr error

	backoff := 10 * time.Millisecond
	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		res, err := c.attempt(ctx, method, path, body)
		res.Attempts = attempt

		if err == nil && res.StatusCode < 500 {
			// 2xx/3xx succeed outright; 4xx is a client error and isn't
			// retryable either way, so both return here.
			return res, nil
		}

		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("client: server error, status %d", res.StatusCode)
		}

		if attempt == c.maxAttempts {
			return res, lastErr
		}

		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return res, ctx.Err()
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}

	return Result{Attempts: c.maxAttempts}, lastErr
}

func (c *Client) attempt(ctx context.Context, method, path string, body []byte) (Result, error) {
	attemptCtx, cancel := context.WithTimeout(ctx, c.attemptTimeout)
	defer cancel()

	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(attemptCtx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return Result{}, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return Result{}, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return Result{}, err
	}

	return Result{StatusCode: resp.StatusCode, Body: respBody}, nil
}

// CloseIdleConnections releases any idle connections held by the client.
func (c *Client) CloseIdleConnections() {
	c.httpClient.CloseIdleConnections()
}
