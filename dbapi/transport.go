package dbapi

import (
	"net/http"

	"github.com/hailocab/go-hostpool"
	"github.com/scylladb/mermaid/log"
)

// transport is an http.RoundTriper that updates request host from context and
// invokes parent RoundTriper.
type transport struct {
	parent http.RoundTripper
	pool   hostpool.HostPool
	logger log.Logger
}

func (t transport) RoundTrip(r *http.Request) (*http.Response, error) {
	ctx := r.Context()

	var (
		h   string
		hpr hostpool.HostPoolResponse
	)

	// get host from context
	h, ok := ctx.Value(_host).(string)

	// get host from pool
	if !ok {
		hpr = t.pool.Get()
		h = hpr.Host()
	}

	r.Host = h
	r.URL.Host = h

	t.logger.Debug(ctx, "Request", "URL", r.URL)

	resp, err := t.parent.RoundTrip(r)
	if resp != nil {
		t.logger.Debug(ctx, "Response",
			"URL", r.URL,
			"StatusCode", resp.StatusCode,
		)
		// force JSON, Scylla returns "text/plain" that misleads the
		// unmarshaller and breaks processing.
		resp.Header.Set("Content-Type", "application/json")
	}

	// mark response
	if hpr != nil {
		hpr.Mark(err)
	}

	return resp, err
}
