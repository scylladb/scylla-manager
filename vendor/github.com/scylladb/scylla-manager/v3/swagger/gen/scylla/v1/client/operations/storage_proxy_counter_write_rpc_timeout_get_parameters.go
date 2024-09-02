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

// NewStorageProxyCounterWriteRPCTimeoutGetParams creates a new StorageProxyCounterWriteRPCTimeoutGetParams object
// with the default values initialized.
func NewStorageProxyCounterWriteRPCTimeoutGetParams() *StorageProxyCounterWriteRPCTimeoutGetParams {

	return &StorageProxyCounterWriteRPCTimeoutGetParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewStorageProxyCounterWriteRPCTimeoutGetParamsWithTimeout creates a new StorageProxyCounterWriteRPCTimeoutGetParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewStorageProxyCounterWriteRPCTimeoutGetParamsWithTimeout(timeout time.Duration) *StorageProxyCounterWriteRPCTimeoutGetParams {

	return &StorageProxyCounterWriteRPCTimeoutGetParams{

		timeout: timeout,
	}
}

// NewStorageProxyCounterWriteRPCTimeoutGetParamsWithContext creates a new StorageProxyCounterWriteRPCTimeoutGetParams object
// with the default values initialized, and the ability to set a context for a request
func NewStorageProxyCounterWriteRPCTimeoutGetParamsWithContext(ctx context.Context) *StorageProxyCounterWriteRPCTimeoutGetParams {

	return &StorageProxyCounterWriteRPCTimeoutGetParams{

		Context: ctx,
	}
}

// NewStorageProxyCounterWriteRPCTimeoutGetParamsWithHTTPClient creates a new StorageProxyCounterWriteRPCTimeoutGetParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewStorageProxyCounterWriteRPCTimeoutGetParamsWithHTTPClient(client *http.Client) *StorageProxyCounterWriteRPCTimeoutGetParams {

	return &StorageProxyCounterWriteRPCTimeoutGetParams{
		HTTPClient: client,
	}
}

/*
StorageProxyCounterWriteRPCTimeoutGetParams contains all the parameters to send to the API endpoint
for the storage proxy counter write Rpc timeout get operation typically these are written to a http.Request
*/
type StorageProxyCounterWriteRPCTimeoutGetParams struct {
	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the storage proxy counter write Rpc timeout get params
func (o *StorageProxyCounterWriteRPCTimeoutGetParams) WithTimeout(timeout time.Duration) *StorageProxyCounterWriteRPCTimeoutGetParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the storage proxy counter write Rpc timeout get params
func (o *StorageProxyCounterWriteRPCTimeoutGetParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the storage proxy counter write Rpc timeout get params
func (o *StorageProxyCounterWriteRPCTimeoutGetParams) WithContext(ctx context.Context) *StorageProxyCounterWriteRPCTimeoutGetParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the storage proxy counter write Rpc timeout get params
func (o *StorageProxyCounterWriteRPCTimeoutGetParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the storage proxy counter write Rpc timeout get params
func (o *StorageProxyCounterWriteRPCTimeoutGetParams) WithHTTPClient(client *http.Client) *StorageProxyCounterWriteRPCTimeoutGetParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the storage proxy counter write Rpc timeout get params
func (o *StorageProxyCounterWriteRPCTimeoutGetParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WriteToRequest writes these params to a swagger request
func (o *StorageProxyCounterWriteRPCTimeoutGetParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
