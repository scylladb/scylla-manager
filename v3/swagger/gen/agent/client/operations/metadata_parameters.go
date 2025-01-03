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

// NewMetadataParams creates a new MetadataParams object
// with the default values initialized.
func NewMetadataParams() *MetadataParams {

	return &MetadataParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewMetadataParamsWithTimeout creates a new MetadataParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewMetadataParamsWithTimeout(timeout time.Duration) *MetadataParams {

	return &MetadataParams{

		timeout: timeout,
	}
}

// NewMetadataParamsWithContext creates a new MetadataParams object
// with the default values initialized, and the ability to set a context for a request
func NewMetadataParamsWithContext(ctx context.Context) *MetadataParams {

	return &MetadataParams{

		Context: ctx,
	}
}

// NewMetadataParamsWithHTTPClient creates a new MetadataParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewMetadataParamsWithHTTPClient(client *http.Client) *MetadataParams {

	return &MetadataParams{
		HTTPClient: client,
	}
}

/*
MetadataParams contains all the parameters to send to the API endpoint
for the metadata operation typically these are written to a http.Request
*/
type MetadataParams struct {
	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the metadata params
func (o *MetadataParams) WithTimeout(timeout time.Duration) *MetadataParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the metadata params
func (o *MetadataParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the metadata params
func (o *MetadataParams) WithContext(ctx context.Context) *MetadataParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the metadata params
func (o *MetadataParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the metadata params
func (o *MetadataParams) WithHTTPClient(client *http.Client) *MetadataParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the metadata params
func (o *MetadataParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WriteToRequest writes these params to a swagger request
func (o *MetadataParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
