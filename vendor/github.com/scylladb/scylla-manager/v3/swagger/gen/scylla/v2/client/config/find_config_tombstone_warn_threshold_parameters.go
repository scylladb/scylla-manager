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

// NewFindConfigTombstoneWarnThresholdParams creates a new FindConfigTombstoneWarnThresholdParams object
// with the default values initialized.
func NewFindConfigTombstoneWarnThresholdParams() *FindConfigTombstoneWarnThresholdParams {

	return &FindConfigTombstoneWarnThresholdParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewFindConfigTombstoneWarnThresholdParamsWithTimeout creates a new FindConfigTombstoneWarnThresholdParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewFindConfigTombstoneWarnThresholdParamsWithTimeout(timeout time.Duration) *FindConfigTombstoneWarnThresholdParams {

	return &FindConfigTombstoneWarnThresholdParams{

		timeout: timeout,
	}
}

// NewFindConfigTombstoneWarnThresholdParamsWithContext creates a new FindConfigTombstoneWarnThresholdParams object
// with the default values initialized, and the ability to set a context for a request
func NewFindConfigTombstoneWarnThresholdParamsWithContext(ctx context.Context) *FindConfigTombstoneWarnThresholdParams {

	return &FindConfigTombstoneWarnThresholdParams{

		Context: ctx,
	}
}

// NewFindConfigTombstoneWarnThresholdParamsWithHTTPClient creates a new FindConfigTombstoneWarnThresholdParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewFindConfigTombstoneWarnThresholdParamsWithHTTPClient(client *http.Client) *FindConfigTombstoneWarnThresholdParams {

	return &FindConfigTombstoneWarnThresholdParams{
		HTTPClient: client,
	}
}

/*
FindConfigTombstoneWarnThresholdParams contains all the parameters to send to the API endpoint
for the find config tombstone warn threshold operation typically these are written to a http.Request
*/
type FindConfigTombstoneWarnThresholdParams struct {
	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the find config tombstone warn threshold params
func (o *FindConfigTombstoneWarnThresholdParams) WithTimeout(timeout time.Duration) *FindConfigTombstoneWarnThresholdParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the find config tombstone warn threshold params
func (o *FindConfigTombstoneWarnThresholdParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the find config tombstone warn threshold params
func (o *FindConfigTombstoneWarnThresholdParams) WithContext(ctx context.Context) *FindConfigTombstoneWarnThresholdParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the find config tombstone warn threshold params
func (o *FindConfigTombstoneWarnThresholdParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the find config tombstone warn threshold params
func (o *FindConfigTombstoneWarnThresholdParams) WithHTTPClient(client *http.Client) *FindConfigTombstoneWarnThresholdParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the find config tombstone warn threshold params
func (o *FindConfigTombstoneWarnThresholdParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WriteToRequest writes these params to a swagger request
func (o *FindConfigTombstoneWarnThresholdParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
