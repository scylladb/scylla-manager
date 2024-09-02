// Code generated by go-swagger; DO NOT EDIT.

package operations

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"context"
	"net/http"
	"time"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/runtime"
	cr "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
)

// NewCacheServiceMetricsCounterRequestsGetParams creates a new CacheServiceMetricsCounterRequestsGetParams object
// with the default values initialized.
func NewCacheServiceMetricsCounterRequestsGetParams() *CacheServiceMetricsCounterRequestsGetParams {

	return &CacheServiceMetricsCounterRequestsGetParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewCacheServiceMetricsCounterRequestsGetParamsWithTimeout creates a new CacheServiceMetricsCounterRequestsGetParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewCacheServiceMetricsCounterRequestsGetParamsWithTimeout(timeout time.Duration) *CacheServiceMetricsCounterRequestsGetParams {

	return &CacheServiceMetricsCounterRequestsGetParams{

		timeout: timeout,
	}
}

// NewCacheServiceMetricsCounterRequestsGetParamsWithContext creates a new CacheServiceMetricsCounterRequestsGetParams object
// with the default values initialized, and the ability to set a context for a request
func NewCacheServiceMetricsCounterRequestsGetParamsWithContext(ctx context.Context) *CacheServiceMetricsCounterRequestsGetParams {

	return &CacheServiceMetricsCounterRequestsGetParams{

		Context: ctx,
	}
}

// NewCacheServiceMetricsCounterRequestsGetParamsWithHTTPClient creates a new CacheServiceMetricsCounterRequestsGetParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewCacheServiceMetricsCounterRequestsGetParamsWithHTTPClient(client *http.Client) *CacheServiceMetricsCounterRequestsGetParams {

	return &CacheServiceMetricsCounterRequestsGetParams{
		HTTPClient: client,
	}
}

/*
CacheServiceMetricsCounterRequestsGetParams contains all the parameters to send to the API endpoint
for the cache service metrics counter requests get operation typically these are written to a http.Request
*/
type CacheServiceMetricsCounterRequestsGetParams struct {
	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the cache service metrics counter requests get params
func (o *CacheServiceMetricsCounterRequestsGetParams) WithTimeout(timeout time.Duration) *CacheServiceMetricsCounterRequestsGetParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the cache service metrics counter requests get params
func (o *CacheServiceMetricsCounterRequestsGetParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the cache service metrics counter requests get params
func (o *CacheServiceMetricsCounterRequestsGetParams) WithContext(ctx context.Context) *CacheServiceMetricsCounterRequestsGetParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the cache service metrics counter requests get params
func (o *CacheServiceMetricsCounterRequestsGetParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the cache service metrics counter requests get params
func (o *CacheServiceMetricsCounterRequestsGetParams) WithHTTPClient(client *http.Client) *CacheServiceMetricsCounterRequestsGetParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the cache service metrics counter requests get params
func (o *CacheServiceMetricsCounterRequestsGetParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WriteToRequest writes these params to a swagger request
func (o *CacheServiceMetricsCounterRequestsGetParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
