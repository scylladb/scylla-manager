// Copyright (C) 2017 ScyllaDB
//
// Modified version of github.com/hashicorp/go-retryablehttp.

package retryablehttp

import (
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/pkg/util/retry"
)

var (
	// Default retry configuration
	defaultRetryWaitMin = 1 * time.Second
	defaultRetryWaitMax = 30 * time.Second
	defaultRetryMax     = 4

	// We need to consume response bodies to maintain http connections, but
	// limit the size we consume to respReadLimit.
	respReadLimit = int64(4096)
)

// ReaderFunc is the type of function that can be given natively to NewRequest
type ReaderFunc func() (io.Reader, error)

// LenReader is an interface implemented by many in-memory io.Reader's. Used
// for automatically sending the right Content-Length header when possible.
type LenReader interface {
	Len() int
}

// CheckRetry specifies a policy for handling retries. It is called
// following each request with the response and error values returned by
// the http.Client. If CheckRetry returns false, the Client stops retrying
// and returns the response to the caller. If CheckRetry returns an error,
// that error value is returned in lieu of the error from the request. The
// Client will close any response body when retrying, but if the retry is
// aborted it is up to the CheckResponse callback to properly close any
// response body before returning.
type CheckRetry func(req *http.Request, resp *http.Response, err error) (bool, error)

// ErrorHandler is called if retries are expired, containing the last status
// from the http library. If not specified, default behavior for the library is
// to close the body and return an error indicating how many tries were
// attempted. If overriding this, be sure to close the body if needed.
type ErrorHandler func(resp *http.Response, err error, numTries int) (*http.Response, error)

// Transport is used to make HTTP requests. It adds additional functionality
// like automatic retries to tolerate minor outages.
type Transport struct {
	parent http.RoundTripper
	logger log.Logger

	// CheckRetry specifies the policy for handling retries, and is called
	// after each request. The default policy is DefaultRetryPolicy.
	CheckRetry CheckRetry

	// NewBackoff returns policy for how long to wait between retries.
	// It's needed because strategy is shared between requests, but state
	// is not.
	NewBackoff func() retry.Backoff

	// ErrorHandler specifies the custom error handler to use, if any
	ErrorHandler ErrorHandler
}

// NewTransport creates a new Transport with default settings.
func NewTransport(parent http.RoundTripper, logger log.Logger) *Transport {
	return &Transport{
		parent:     parent,
		logger:     logger,
		CheckRetry: DefaultRetryPolicy,
		NewBackoff: func() retry.Backoff {
			return DefaultBackoff(defaultRetryWaitMin, defaultRetryWaitMax, defaultRetryMax)
		},
	}
}

// DefaultBackoff provides a default backoff strategy that
// will perform exponential backoff with limited number of retries.
// Minimum and maximum durations are limited by provided parameters.
func DefaultBackoff(waitMin, waitMax time.Duration, retryMax int) retry.Backoff {
	maxElapsedTime := time.Duration(retryMax) * waitMax
	multiplier := 2.0
	randomFactor := 0.0

	return retry.WithMaxRetries(
		retry.NewExponentialBackoff(waitMin, maxElapsedTime, waitMax, multiplier, randomFactor),
		uint64(retryMax),
	)
}

// DefaultRetryPolicy provides a default callback for Client.CheckRetry.
// It will retry on server errors and connection errors that don't include
// connection resets.
func DefaultRetryPolicy(req *http.Request, resp *http.Response, err error) (bool, error) {
	if req != nil {
		if err := req.Context().Err(); err != nil {
			return false, err
		}
	}

	if err != nil {
		// RoundTripper can't handle connection resets so we are testing for
		// such errors
		switch t := err.(type) {
		case *net.OpError:
			if t.Op == "read" {
				return false, err
			}
		default:
			if err == io.EOF {
				return false, err
			}
		}
		return true, err
	}
	// Check the response code. We retry on 500-range responses to allow
	// the server time to recover, as 500's are typically not permanent
	// errors and may relate to outages on the server side. This will catch
	// invalid response codes as well, like 0 and 999.
	if resp.StatusCode == 0 || (resp.StatusCode >= 500 && resp.StatusCode != 501) {
		return true, nil
	}

	return false, nil
}

// RoundTrip wraps calling parent RoundTrip method with retries.
func (t Transport) RoundTrip(req *http.Request) (*http.Response, error) {
	var (
		r       *http.Request
		resp    *http.Response
		err     error
		retries = -1
	)

	// Copy the request if contains body
	if req.Body != nil {
		r = new(http.Request)
		*r = *req
		r.Body = &body{req.Body, false}
	} else {
		r = req
	}

	// Because backoff is set per RoundTripper, and each RT handles multiple
	// requests concurrently, same object can not be used due to internal backoff
	// state.
	backoff := t.NewBackoff()

	notify := func(e error, wait time.Duration) {
		t.logger.Info(r.Context(), "retrying",
			"host", r.Host,
			"method", r.Method,
			"uri", r.URL.RequestURI(),
			"wait", wait,
			"error", e,
		)

		// We're going to retry, consume any response to reuse the connection.
		if err == nil && resp != nil {
			t.drainBody(resp)
		}
	}

	op := func() error {
		retries++
		// Attempt the request.
		// Body is closed in notify function which isn't called after last retry.
		// This property allows to return non-consumed body back to user, and
		// consume it only when next retry happens.
		resp, err = t.parent.RoundTrip(r) //nolint: bodyclose
		if err != nil {
			t.logger.Info(r.Context(), "request failed",
				"host", r.Host,
				"method", r.Method,
				"uri", r.URL.RequestURI(),
				"error", err.Error(),
			)
		}

		// If body was read do not continue.
		if b, ok := r.Body.(*body); ok && b.read {
			return retry.Permanent(err)
		}

		// Check if we should continue with retries.
		checkOK, checkErr := t.CheckRetry(r, resp, err)

		// Now decide if we should continue.
		if !checkOK {
			if checkErr != nil {
				err = checkErr
			}
			return retry.Permanent(err)
		}

		if err != nil {
			return err
		}
		return errors.Errorf("unexpected response code: %d", resp.StatusCode)
	}

	// Error is handled by operation
	_ = retry.WithNotify(req.Context(), op, backoff, notify) //nolint:errcheck
	return resp, errors.Wrapf(err, "giving up after %d retries", retries)
}

// Try to read the response body so we can reuse this connection.
func (t *Transport) drainBody(resp *http.Response) {
	defer resp.Body.Close()
	_, err := io.Copy(ioutil.Discard, io.LimitReader(resp.Body, respReadLimit))
	if err != nil {
		t.logger.Info(resp.Request.Context(), "error reading response body", "error", err)
	}
}

type body struct {
	io.ReadCloser
	read bool
}

func (c *body) Read(bs []byte) (int, error) {
	if !c.read {
		c.read = true
	}
	return c.ReadCloser.Read(bs)
}
