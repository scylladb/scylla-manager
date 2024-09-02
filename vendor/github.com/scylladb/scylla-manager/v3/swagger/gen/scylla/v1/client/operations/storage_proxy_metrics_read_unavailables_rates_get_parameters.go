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

// NewStorageProxyMetricsReadUnavailablesRatesGetParams creates a new StorageProxyMetricsReadUnavailablesRatesGetParams object
// with the default values initialized.
func NewStorageProxyMetricsReadUnavailablesRatesGetParams() *StorageProxyMetricsReadUnavailablesRatesGetParams {

	return &StorageProxyMetricsReadUnavailablesRatesGetParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewStorageProxyMetricsReadUnavailablesRatesGetParamsWithTimeout creates a new StorageProxyMetricsReadUnavailablesRatesGetParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewStorageProxyMetricsReadUnavailablesRatesGetParamsWithTimeout(timeout time.Duration) *StorageProxyMetricsReadUnavailablesRatesGetParams {

	return &StorageProxyMetricsReadUnavailablesRatesGetParams{

		timeout: timeout,
	}
}

// NewStorageProxyMetricsReadUnavailablesRatesGetParamsWithContext creates a new StorageProxyMetricsReadUnavailablesRatesGetParams object
// with the default values initialized, and the ability to set a context for a request
func NewStorageProxyMetricsReadUnavailablesRatesGetParamsWithContext(ctx context.Context) *StorageProxyMetricsReadUnavailablesRatesGetParams {

	return &StorageProxyMetricsReadUnavailablesRatesGetParams{

		Context: ctx,
	}
}

// NewStorageProxyMetricsReadUnavailablesRatesGetParamsWithHTTPClient creates a new StorageProxyMetricsReadUnavailablesRatesGetParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewStorageProxyMetricsReadUnavailablesRatesGetParamsWithHTTPClient(client *http.Client) *StorageProxyMetricsReadUnavailablesRatesGetParams {

	return &StorageProxyMetricsReadUnavailablesRatesGetParams{
		HTTPClient: client,
	}
}

/*
StorageProxyMetricsReadUnavailablesRatesGetParams contains all the parameters to send to the API endpoint
for the storage proxy metrics read unavailables rates get operation typically these are written to a http.Request
*/
type StorageProxyMetricsReadUnavailablesRatesGetParams struct {
	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the storage proxy metrics read unavailables rates get params
func (o *StorageProxyMetricsReadUnavailablesRatesGetParams) WithTimeout(timeout time.Duration) *StorageProxyMetricsReadUnavailablesRatesGetParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the storage proxy metrics read unavailables rates get params
func (o *StorageProxyMetricsReadUnavailablesRatesGetParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the storage proxy metrics read unavailables rates get params
func (o *StorageProxyMetricsReadUnavailablesRatesGetParams) WithContext(ctx context.Context) *StorageProxyMetricsReadUnavailablesRatesGetParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the storage proxy metrics read unavailables rates get params
func (o *StorageProxyMetricsReadUnavailablesRatesGetParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the storage proxy metrics read unavailables rates get params
func (o *StorageProxyMetricsReadUnavailablesRatesGetParams) WithHTTPClient(client *http.Client) *StorageProxyMetricsReadUnavailablesRatesGetParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the storage proxy metrics read unavailables rates get params
func (o *StorageProxyMetricsReadUnavailablesRatesGetParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WriteToRequest writes these params to a swagger request
func (o *StorageProxyMetricsReadUnavailablesRatesGetParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
