// Copyright (C) 2017 ScyllaDB

package httpmw

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
)

// body defers context cancellation until response body is closed.
type body struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b body) Close() error {
	defer b.cancel()
	return b.ReadCloser.Close()
}

// Timeout sets request context timeout for individual requests.
func Timeout(next http.RoundTripper, timeout time.Duration, logger log.Logger) http.RoundTripper {
	return RoundTripperFunc(func(req *http.Request) (resp *http.Response, err error) {
		ctx, cancel := context.WithTimeout(req.Context(), timeout)
		defer func() {
			if resp != nil {
				resp.Body = body{
					ReadCloser: resp.Body,
					cancel:     cancel,
				}
			}

			if errors.Cause(err) == context.DeadlineExceeded && ctx.Err() == context.DeadlineExceeded {
				err = errors.Errorf("timeout after %s", timeout)
			}
		}()
		return next.RoundTrip(req.WithContext(ctx))
	})
}
