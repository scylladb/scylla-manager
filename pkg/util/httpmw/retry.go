package httpmw

import (
	"net/http"
	"time"

	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/pkg/util/retry"
	"github.com/scylladb/mermaid/pkg/util/retryablehttp"
)

// Retry retries request if needed.
func Retry(next http.RoundTripper, poolSize int, logger log.Logger) http.RoundTripper {
	// Retry policy while using a specified host.
	hostRetry := retryablehttp.NewTransport(next, logger)

	// Retry policy while using host pool, no backoff wait time, switch host as
	// fast as possible.
	poolRetry := retryablehttp.NewTransport(next, logger)
	// DefaultRetryPolicy uses special logic not to retry on broken connections,
	// here as we switch hosts we want to always retry.
	poolRetry.CheckRetry = func(req *http.Request, resp *http.Response, err error) (bool, error) {
		if req != nil {
			if err := req.Context().Err(); err != nil {
				return false, err
			}
		}
		if err != nil {
			return true, err
		}
		if c := resp.StatusCode; c == 0 || c == 401 || c == 403 || (c >= 500 && c != 501) {
			return true, nil
		}
		return false, nil
	}
	poolRetry.NewBackoff = func() retry.Backoff {
		return retry.WithMaxRetries(retry.BackoffFunc(func() time.Duration {
			return 0
		}), uint64(poolSize))
	}

	return RoundTripperFunc(func(req *http.Request) (resp *http.Response, err error) {
		if _, ok := req.Context().Value(ctxNoRetry).(bool); ok {
			return next.RoundTrip(req)
		}

		if _, ok := req.Context().Value(ctxHost).(string); ok {
			return hostRetry.RoundTrip(req)
		}

		return poolRetry.RoundTrip(req)
	})
}
