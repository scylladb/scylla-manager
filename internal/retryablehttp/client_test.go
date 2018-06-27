// Copyright (C) 2017 ScyllaDB
//
// Modified version of github.com/hashicorp/go-retryablehttp.

package retryablehttp

import (
	"bytes"
	"errors"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/golog"
)

// Testing helpers
func NewClient() (*http.Client, *Transport) {
	t := NewTransport(http.DefaultTransport, golog.NopLogger)
	return &http.Client{
		Transport: t,
	}, t
}

var NewRequest = http.NewRequest

// Since normal ways we would generate a Reader have special cases, use a
// custom type here
type custReader struct {
	val string
	pos int
}

func (c *custReader) Read(p []byte) (n int, err error) {
	if c.val == "" {
		c.val = "hello"
	}
	if c.pos >= len(c.val) {
		return 0, io.EOF
	}
	var i int
	for i = 0; i < len(p) && i+c.pos < len(c.val); i++ {
		p[i] = c.val[i+c.pos]
	}
	c.pos += i
	return i, nil
}

func TestClient_Do(t *testing.T) {
	testBytes := []byte("hello")
	body := ioutil.NopCloser(bytes.NewReader(testBytes))

	// Create a request
	req, err := NewRequest("PUT", "http://127.0.0.1:28934/v1/foo", body)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	req.Header.Set("foo", "bar")

	// Create the client. Use short retry windows.
	client, transport := NewClient()
	transport.RetryWaitMin = 10 * time.Millisecond
	transport.RetryWaitMax = 50 * time.Millisecond
	transport.RetryMax = 50

	// Send the request
	var resp *http.Response
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		var err error
		resp, err = client.Do(req)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
	}()

	select {
	case <-doneCh:
		t.Fatalf("should retry on error")
	case <-time.After(200 * time.Millisecond):
		// Client should still be retrying due to connection failure.
	}

	// Create the mock handler. First we return a 500-range response to ensure
	// that we power through and keep retrying in the face of recoverable
	// errors.
	code := int64(200)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check the request details
		if r.Method != "PUT" {
			t.Fatalf("bad method: %s", r.Method)
		}
		if r.RequestURI != "/v1/foo" {
			t.Fatalf("bad uri: %s", r.RequestURI)
		}

		// Check the headers
		if v := r.Header.Get("foo"); v != "bar" {
			t.Fatalf("bad header: expect foo=bar, got foo=%v", v)
		}

		// Check the payload
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("err: %s", err)
		}
		expected := []byte("hello")
		if !bytes.Equal(body, expected) {
			t.Fatalf("bad: %v", body)
		}

		w.WriteHeader(int(atomic.LoadInt64(&code)))
	})

	// Create a test server
	list, err := net.Listen("tcp", ":28934")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer list.Close()
	go http.Serve(list, handler)

	// Wait again
	select {
	case <-doneCh:
	case <-time.After(time.Second):
		t.Fatalf("timed out")
	}

	if resp.StatusCode != 200 {
		t.Fatalf("exected 200, got: %d", resp.StatusCode)
	}
}

func TestClient_Do_fails(t *testing.T) {
	// Mock server which always responds 500.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer ts.Close()

	// Create the client. Use short retry windows so we fail faster.
	client, transport := NewClient()
	transport.RetryWaitMin = 10 * time.Millisecond
	transport.RetryWaitMax = 10 * time.Millisecond
	transport.RetryMax = 2

	// Create the request
	req, err := NewRequest("POST", ts.URL, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// Send the request.
	_, err = client.Do(req)
	if err == nil || !strings.Contains(err.Error(), "giving up") {
		t.Fatalf("expected giving up error, got: %#v", err)
	}
}

func TestClient_CheckRetry(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "test_500_body", http.StatusInternalServerError)
	}))
	defer ts.Close()

	client, transport := NewClient()

	retryErr := errors.New("retryError")
	called := 0
	transport.CheckRetry = func(resp *http.Response, err error) (bool, error) {
		if called < 1 {
			called++
			return DefaultRetryPolicy(resp, err)
		}

		return false, retryErr
	}

	// CheckRetry should return our retryErr value and stop the retry loop.
	_, err := client.Get(ts.URL)

	if called != 1 {
		t.Fatalf("CheckRetry called %d times, expected 1", called)
	}

	if err == nil || !strings.HasSuffix(err.Error(), ": retryError") {
		t.Fatalf("Expected retryError, got:%v", err)
	}
}

func TestClient_CheckRetryStop(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "test_500_body", http.StatusInternalServerError)
	}))
	defer ts.Close()

	client, transport := NewClient()

	// Verify that this stops retries on the first try, with no errors from the client.
	called := 0
	transport.CheckRetry = func(resp *http.Response, err error) (bool, error) {
		called++
		return false, nil
	}

	_, err := client.Get(ts.URL)

	if called != 1 {
		t.Fatalf("CheckRetry called %d times, expeted 1", called)
	}

	if err != nil {
		t.Fatalf("Expected no error, got:%v", err)
	}
}

func TestBackoff(t *testing.T) {
	type tcase struct {
		min    time.Duration
		max    time.Duration
		i      int
		expect time.Duration
	}
	cases := []tcase{
		{
			time.Second,
			5 * time.Minute,
			0,
			time.Second,
		},
		{
			time.Second,
			5 * time.Minute,
			1,
			2 * time.Second,
		},
		{
			time.Second,
			5 * time.Minute,
			2,
			4 * time.Second,
		},
		{
			time.Second,
			5 * time.Minute,
			3,
			8 * time.Second,
		},
		{
			time.Second,
			5 * time.Minute,
			63,
			5 * time.Minute,
		},
		{
			time.Second,
			5 * time.Minute,
			128,
			5 * time.Minute,
		},
	}

	for _, tc := range cases {
		if v := DefaultBackoff(tc.min, tc.max, tc.i, nil); v != tc.expect {
			t.Fatalf("bad: %#v -> %s", tc, v)
		}
	}
}

func TestClient_BackoffCustom(t *testing.T) {
	var retries int32

	client, transport := NewClient()
	transport.Backoff = func(min, max time.Duration, attemptNum int, resp *http.Response) time.Duration {
		atomic.AddInt32(&retries, 1)
		return time.Millisecond * 1
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.LoadInt32(&retries) == int32(transport.RetryMax) {
			w.WriteHeader(200)
			return
		}
		w.WriteHeader(500)
	}))
	defer ts.Close()

	// Make the request.
	resp, err := client.Get(ts.URL + "/foo/bar")
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	resp.Body.Close()
	if retries != int32(transport.RetryMax) {
		t.Fatalf("expected retries: %d != %d", transport.RetryMax, retries)
	}
}
