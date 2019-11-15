// Copyright (C) 2017 ScyllaDB

package httpmw

import (
	"context"
	"io"
	"net/http"
	"time"

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
				resp.Body = body{resp.Body, cancel}
			}
			if ctx.Err() != nil {
				logger.Info(ctx, "Transport request timeout", "timeout", timeout, "err", ctx.Err())
			}
		}()
		return next.RoundTrip(req.WithContext(ctx))
	})
}
