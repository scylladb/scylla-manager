package dbapi

import (
	"errors"
	"net/http"

	"github.com/scylladb/mermaid/log"
)

// transport is an http.RoundTriper that updates request host from context and
// invokes parent RoundTriper.
type transport struct {
	parent http.RoundTripper
	logger log.Logger
}

func (t transport) RoundTrip(r *http.Request) (*http.Response, error) {
	ctx := r.Context()

	h, ok := ctx.Value(_host).(string)
	if !ok {
		return nil, errors.New("no host in context")
	}
	r.Host = h
	r.URL.Host = h

	t.logger.Debug(ctx, "request",
		"host", h,
		"method", r.Method,
		"path", r.URL.Path,
	)

	resp, err := t.parent.RoundTrip(r)
	if resp != nil {
		t.logger.Debug(ctx, "response",
			"host", h,
			"method", r.Method,
			"path", r.URL.Path,
			"status", resp.StatusCode,
		)
		// Force JSON, Scylla returns "text/plain" that misleads the
		// unmarshaller and breaks processing.
		resp.Header.Set("Content-Type", "application/json")
	}

	return resp, err
}
