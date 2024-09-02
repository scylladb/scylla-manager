// Code generated by go-swagger; DO NOT EDIT.

package config

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

// NewFindConfigTruncateRequestTimeoutInMsParams creates a new FindConfigTruncateRequestTimeoutInMsParams object
// with the default values initialized.
func NewFindConfigTruncateRequestTimeoutInMsParams() *FindConfigTruncateRequestTimeoutInMsParams {

	return &FindConfigTruncateRequestTimeoutInMsParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewFindConfigTruncateRequestTimeoutInMsParamsWithTimeout creates a new FindConfigTruncateRequestTimeoutInMsParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewFindConfigTruncateRequestTimeoutInMsParamsWithTimeout(timeout time.Duration) *FindConfigTruncateRequestTimeoutInMsParams {

	return &FindConfigTruncateRequestTimeoutInMsParams{

		timeout: timeout,
	}
}

// NewFindConfigTruncateRequestTimeoutInMsParamsWithContext creates a new FindConfigTruncateRequestTimeoutInMsParams object
// with the default values initialized, and the ability to set a context for a request
func NewFindConfigTruncateRequestTimeoutInMsParamsWithContext(ctx context.Context) *FindConfigTruncateRequestTimeoutInMsParams {

	return &FindConfigTruncateRequestTimeoutInMsParams{

		Context: ctx,
	}
}

// NewFindConfigTruncateRequestTimeoutInMsParamsWithHTTPClient creates a new FindConfigTruncateRequestTimeoutInMsParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewFindConfigTruncateRequestTimeoutInMsParamsWithHTTPClient(client *http.Client) *FindConfigTruncateRequestTimeoutInMsParams {

	return &FindConfigTruncateRequestTimeoutInMsParams{
		HTTPClient: client,
	}
}

/*
FindConfigTruncateRequestTimeoutInMsParams contains all the parameters to send to the API endpoint
for the find config truncate request timeout in ms operation typically these are written to a http.Request
*/
type FindConfigTruncateRequestTimeoutInMsParams struct {
	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the find config truncate request timeout in ms params
func (o *FindConfigTruncateRequestTimeoutInMsParams) WithTimeout(timeout time.Duration) *FindConfigTruncateRequestTimeoutInMsParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the find config truncate request timeout in ms params
func (o *FindConfigTruncateRequestTimeoutInMsParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the find config truncate request timeout in ms params
func (o *FindConfigTruncateRequestTimeoutInMsParams) WithContext(ctx context.Context) *FindConfigTruncateRequestTimeoutInMsParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the find config truncate request timeout in ms params
func (o *FindConfigTruncateRequestTimeoutInMsParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the find config truncate request timeout in ms params
func (o *FindConfigTruncateRequestTimeoutInMsParams) WithHTTPClient(client *http.Client) *FindConfigTruncateRequestTimeoutInMsParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the find config truncate request timeout in ms params
func (o *FindConfigTruncateRequestTimeoutInMsParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WriteToRequest writes these params to a swagger request
func (o *FindConfigTruncateRequestTimeoutInMsParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
