// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"context"
	"net/url"
	"time"

	"github.com/go-openapi/runtime"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/pkg/util/retry"
)

type retryConfig struct {
	normal      BackoffConfig
	interactive BackoffConfig
	poolSize    int
	timeout     time.Duration
}

func newRetryConfig(config Config) *retryConfig {
	return &retryConfig{
		normal:      config.Backoff,
		interactive: config.InteractiveBackoff,
		poolSize:    len(config.Hosts),
		timeout:     config.Timeout,
	}
}

func (c *retryConfig) backoff(ctx context.Context) retry.Backoff {
	if isForceHost(ctx) {
		if isInteractive(ctx) {
			return backoff(c.interactive)
		}
		return backoff(c.normal)
	}

	// We want to send request to every host in the pool once.
	// The -1 is to avoid reaching out to the first node - that failed.
	maxRetries := c.poolSize - 1
	return noBackoff(maxRetries)
}

func backoff(config BackoffConfig) retry.Backoff {
	return retry.WithMaxRetries(retry.NewExponentialBackoff(
		config.WaitMin,
		0,
		config.WaitMax,
		config.Multiplier,
		config.Jitter,
	), config.MaxRetries)
}

func noBackoff(maxRetries int) retry.Backoff {
	return retry.WithMaxRetries(retry.BackoffFunc(func() time.Duration { return 0 }), uint64(maxRetries))
}

type retryableTransport struct {
	transport runtime.ClientTransport
	config    *retryConfig
	logger    log.Logger
}

type retryableOperation struct {
	config   *retryConfig
	ctx      context.Context
	id       string
	result   interface{}
	attempts int
	logger   log.Logger

	do func() (interface{}, error)
}

// retryable wraps parent and adds retry capabilities.
func retryable(transport runtime.ClientTransport, config *retryConfig, logger log.Logger) runtime.ClientTransport {
	return retryableTransport{
		transport: transport,
		config:    config,
		logger:    logger,
	}
}

func (t retryableTransport) Submit(operation *runtime.ClientOperation) (interface{}, error) {
	if _, ok := operation.Context.Value(ctxNoRetry).(bool); ok {
		v, err := t.transport.Submit(operation)
		return v, unpackURLError(err)
	}

	o := &retryableOperation{
		config: t.config,
		ctx:    operation.Context,
		id:     operation.ID,
		logger: t.logger,
	}
	o.do = func() (interface{}, error) {
		operation.Context = o.ctx
		return t.transport.Submit(operation)
	}
	return o.submit()
}

func (o *retryableOperation) submit() (interface{}, error) {
	err := retry.WithNotify(o.ctx, o.op, o.config.backoff(o.ctx), o.notify)
	if err != nil {
		err = unpackURLError(err)

		// Do not print "giving up after 1 attempts" for permanent errors.
		if o.attempts > 1 {
			err = errors.Wrapf(err, "giving up after %d attempts", o.attempts)
		}
		return nil, err
	}
	return o.result, nil
}

func (o *retryableOperation) op() (err error) {
	o.attempts++

	o.result, err = o.do()
	if err != nil {
		if !shouldRetry(o.ctx, err) {
			err = retry.Permanent(err)
			return
		}
		if shouldIncreaseTimeout(o.ctx, err) {
			timeout := 2 * o.config.timeout
			if ct, ok := hasCustomTimeout(o.ctx); ok {
				timeout = ct + o.config.timeout
			}

			o.logger.Debug(o.ctx, "HTTP increasing timeout",
				"operation", o.id,
				"timeout", timeout,
			)
			o.ctx = customTimeout(o.ctx, timeout)
		}
	}

	return
}

func shouldRetry(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return false
	}

	// Check if there is a retry handler attached to the context.
	// If handler cannot decide move on to the default handler.
	if h := shouldRetryHandler(ctx); h != nil {
		if shouldRetry := h(err); shouldRetry != nil {
			return *shouldRetry
		}
	}

	// Check the response code. We retry on 500-range responses to allow
	// the server time to recover, as 500's are typically not permanent
	// errors and may relate to outages on the server side. This will catch
	// invalid response codes as well, like 0 and 999.
	c := StatusCodeOf(err)
	if c == 0 || (c >= 500 && c != 501) {
		return true
	}

	// Additionally, if request can be resent to a different host retry
	// on Unauthorized or Forbidden.
	if !isForceHost(ctx) {
		if c == 401 || c == 403 {
			return true
		}
	}

	return false
}

func shouldIncreaseTimeout(ctx context.Context, err error) bool {
	return isForceHost(ctx) && !isInteractive(ctx) && errors.Is(err, ErrTimeout)
}

func (o *retryableOperation) notify(err error, wait time.Duration) {
	if wait == 0 {
		o.logger.Info(o.ctx, "HTTP retry now",
			"operation", o.id,
			"error", unpackURLError(err),
		)
	} else {
		o.logger.Info(o.ctx, "HTTP retry backoff",
			"operation", o.id,
			"wait", wait,
			"error", unpackURLError(err),
		)
	}
}

func unpackURLError(err error) error {
	if e, ok := err.(*url.Error); ok { // nolint: errorlint
		return e.Err
	}

	return err
}
