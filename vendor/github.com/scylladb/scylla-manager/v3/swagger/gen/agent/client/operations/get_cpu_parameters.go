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

// NewGetCPUParams creates a new GetCPUParams object
// with the default values initialized.
func NewGetCPUParams() *GetCPUParams {

	return &GetCPUParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewGetCPUParamsWithTimeout creates a new GetCPUParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewGetCPUParamsWithTimeout(timeout time.Duration) *GetCPUParams {

	return &GetCPUParams{

		timeout: timeout,
	}
}

// NewGetCPUParamsWithContext creates a new GetCPUParams object
// with the default values initialized, and the ability to set a context for a request
func NewGetCPUParamsWithContext(ctx context.Context) *GetCPUParams {

	return &GetCPUParams{

		Context: ctx,
	}
}

// NewGetCPUParamsWithHTTPClient creates a new GetCPUParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewGetCPUParamsWithHTTPClient(client *http.Client) *GetCPUParams {

	return &GetCPUParams{
		HTTPClient: client,
	}
}

/*
GetCPUParams contains all the parameters to send to the API endpoint
for the get Cpu operation typically these are written to a http.Request
*/
type GetCPUParams struct {
	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the get Cpu params
func (o *GetCPUParams) WithTimeout(timeout time.Duration) *GetCPUParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the get Cpu params
func (o *GetCPUParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the get Cpu params
func (o *GetCPUParams) WithContext(ctx context.Context) *GetCPUParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the get Cpu params
func (o *GetCPUParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the get Cpu params
func (o *GetCPUParams) WithHTTPClient(client *http.Client) *GetCPUParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the get Cpu params
func (o *GetCPUParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WriteToRequest writes these params to a swagger request
func (o *GetCPUParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
