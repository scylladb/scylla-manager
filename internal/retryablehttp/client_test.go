// Copyright (C) 2017 ScyllaDB
//
// Modified version of github.com/hashicorp/go-retryablehttp.

package retryablehttp

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/go-log"
)

// Testing helpers
func NewClient() (*http.Client, *Transport) {
	t := NewTransport(http.DefaultTransport, log.NopLogger)
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
		t.Fatalf("Err: %v", err)
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
			t.Fatalf("Err: %v", err)
		}
	}()

	select {
	case <-doneCh:
		t.Fatalf("Should retry on error")
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
			t.Fatalf("Bad method: %s", r.Method)
		}
		if r.RequestURI != "/v1/foo" {
			t.Fatalf("Bad uri: %s", r.RequestURI)
		}

		// Check the headers
		if v := r.Header.Get("foo"); v != "bar" {
			t.Fatalf("Bad header: expect foo=bar, got foo=%v", v)
		}

		// Check the payload
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("Err: %s", err)
		}
		expected := []byte("hello")
		if !bytes.Equal(body, expected) {
			t.Fatalf("Bad: %v", body)
		}

		w.WriteHeader(int(atomic.LoadInt64(&code)))
	})

	// Create a test server
	list, err := net.Listen("tcp", ":28934")
	if err != nil {
		t.Fatalf("Err: %v", err)
	}
	defer list.Close()
	go http.Serve(list, handler)

	// Wait again
	select {
	case <-doneCh:
	case <-time.After(time.Second):
		t.Fatalf("Timed out")
	}

	if resp.StatusCode != 200 {
		t.Fatalf("Exected 200, got: %d", resp.StatusCode)
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
		t.Fatalf("Err: %v", err)
	}

	// Send the request.
	_, err = client.Do(req)
	if err == nil || !strings.Contains(err.Error(), "giving up") {
		t.Fatalf("Expected giving up error, got: %#v", err)
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
	transport.CheckRetry = func(req *http.Request, resp *http.Response, err error) (bool, error) {
		if called < 1 {
			called++
			return DefaultRetryPolicy(req, resp, err)
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

func TestClient_CheckCanceled(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "test_500_body", http.StatusInternalServerError)
	}))
	defer ts.Close()

	client, transport := NewClient()
	called := 0

	req, _ := http.NewRequest("GET", ts.URL, nil)
	ctx, cancel := context.WithCancel(req.Context())
	req = req.WithContext(ctx)
	cancel()

	transport.CheckRetry = func(req *http.Request, resp *http.Response, err error) (bool, error) {
		called++
		return DefaultRetryPolicy(req, resp, err)
	}

	client.Do(req)

	if called != 1 {
		t.Fatalf("CheckRetry called %d times, expected 1", called)
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
	transport.CheckRetry = func(req *http.Request, resp *http.Response, err error) (bool, error) {
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

type response struct {
	resp *http.Response
	err  error
}

const (
	testURL  = "http://127.0.0.1:28934/v1/foo"
	testAddr = "127.0.0.1:28934"
)

var (
	testTimeout = 3 * time.Second
	testBytes   = []byte("{}")
)

func TestRetryAfterNoConnection(t *testing.T) {
	results := make(chan response)
	retries := make(chan struct{})
	requests := make(chan struct{})
	body := ioutil.NopCloser(bytes.NewReader(testBytes))

	ts := http.Server{
		Addr: testAddr,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests <- struct{}{}
			w.Header().Add("Content-Type", "application/json")
			fmt.Fprint(w, "{}")
		}),
	}
	defer ts.Close()

	// Create the client. Use short retry windows.
	client, transport := NewClient()
	transport.RetryWaitMin = 10 * time.Millisecond
	transport.RetryWaitMax = 50 * time.Millisecond
	transport.RetryMax = 20
	transport.CheckRetry = func(req *http.Request, resp *http.Response, err error) (bool, error) {
		retry, err := DefaultRetryPolicy(req, resp, err)
		if retry {
			select {
			case retries <- struct{}{}:
			default:
			}
		}
		return retry, err
	}
	sendRequest := func() {
		var (
			resp *http.Response
			err  error
		)
		resp, err = client.Post(testURL, "application/json", body)
		results <- response{
			resp: resp,
			err:  err,
		}
	}

	// Request to server that isn't running.
	go sendRequest()

	select {
	case res := <-results:
		if res.err == nil {
			t.Fatal("first request should fail")
		}
	case <-time.After(testTimeout):
		t.Fatal("time out after first request")
	}

	// Request to server that we are going to start after first retry.
	go sendRequest()

	select {
	case <-retries:
		// Wait for first retry and then start the server.
		go ts.ListenAndServe()
	case <-time.After(testTimeout):
		t.Fatal("time out waiting for retry")
	}

	select {
	case <-requests:
		// Request should be registered on the server.
	case <-time.After(testTimeout):
		t.Fatal("time out receiving request")
	}

	select {
	case res := <-results:
		// Response should have no error.
		if res.err != nil {
			t.Fatalf("Request failed %+v", res.err)
		}
	case <-time.After(testTimeout):
		t.Fatal("time out after second request")
	}
}

func TestRetryAfterInternalServerError(t *testing.T) {
	results := make(chan response)
	retries := make(chan struct{})
	requests := make(chan struct{})
	var fail int64

	ts := http.Server{
		Addr: testAddr,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if atomic.LoadInt64(&fail) == 0 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			select {
			case requests <- struct{}{}:
			default:
			}
			w.Header().Add("Content-Type", "application/json")
			fmt.Fprint(w, "{}")
		}),
	}
	go ts.ListenAndServe()
	defer ts.Close()

	// Create the client. Use short retry windows.
	client, transport := NewClient()
	transport.RetryWaitMin = 10 * time.Millisecond
	transport.RetryWaitMax = 50 * time.Millisecond
	transport.RetryMax = 20
	transport.CheckRetry = func(req *http.Request, resp *http.Response, err error) (bool, error) {
		retry, err := DefaultRetryPolicy(req, resp, err)
		if retry {
			select {
			case retries <- struct{}{}:
			default:
			}
		}
		return retry, err
	}
	sendRequest := func() {
		var (
			resp *http.Response
			err  error
		)
		resp, err = client.Get(testURL)
		results <- response{
			resp: resp,
			err:  err,
		}
	}

	go sendRequest()

	select {
	case <-retries:
		// Retries started stop failing.
		atomic.AddInt64(&fail, 1)
	case <-time.After(testTimeout):
		t.Fatal("time out waiting for retry")
	}

	select {
	case <-requests:
		// Request should be registered on the server.
	case <-time.After(testTimeout):
		t.Fatal("time out receiving request")
	}

	select {
	case res := <-results:
		// Response should have no error.
		if res.err != nil {
			t.Fatalf("Request failed %+v", res.err)
		}
	case <-time.After(testTimeout):
		t.Fatal("time out getting request after retries")
	}
}

func TestRetryAfterConnectionReset(t *testing.T) {
	results := make(chan response)
	retries := make(chan struct{})
	requests := make(chan struct{})

	ts := http.Server{
		Addr: testAddr,
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests <- struct{}{}
			<-retries
		}),
	}
	go ts.ListenAndServe()

	// Create the client. Use short retry windows.
	client, transport := NewClient()
	transport.RetryWaitMin = 10 * time.Millisecond
	transport.RetryWaitMax = 50 * time.Millisecond
	transport.RetryMax = 50
	transport.CheckRetry = func(req *http.Request, resp *http.Response, err error) (bool, error) {
		close(retries)
		retry, err := DefaultRetryPolicy(req, resp, err)
		return retry, err
	}
	sendRequest := func() {
		var (
			resp *http.Response
			err  error
		)
		resp, err = client.Get(testURL)
		results <- response{
			resp: resp,
			err:  err,
		}
	}

	go sendRequest()

	select {
	case <-requests:
		// Bring down server in the middle of a request.
		ts.Close()
		time.Sleep(100 * time.Millisecond)
	case <-time.After(testTimeout):
		t.Fatal("time out receiving first request")
	}

	select {
	case <-retries:
	case <-time.After(testTimeout):
		t.Fatal("time out waiting for retry check")
	}
	select {
	case res := <-results:
		// Response should fail because of network error.
		if res.err == nil {
			t.Errorf("Request should fail")
		}
	case <-time.After(testTimeout):
		t.Fatal("time out getting request after retries")
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
			t.Fatalf("Bad: %#v -> %s", tc, v)
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
		t.Fatalf("Err: %v", err)
	}
	resp.Body.Close()
	if retries != int32(transport.RetryMax) {
		t.Fatalf("Expected retries: %d != %d", transport.RetryMax, retries)
	}
}
