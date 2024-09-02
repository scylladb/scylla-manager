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

// NewFindConfigReplaceTokenParams creates a new FindConfigReplaceTokenParams object
// with the default values initialized.
func NewFindConfigReplaceTokenParams() *FindConfigReplaceTokenParams {

	return &FindConfigReplaceTokenParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewFindConfigReplaceTokenParamsWithTimeout creates a new FindConfigReplaceTokenParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewFindConfigReplaceTokenParamsWithTimeout(timeout time.Duration) *FindConfigReplaceTokenParams {

	return &FindConfigReplaceTokenParams{

		timeout: timeout,
	}
}

// NewFindConfigReplaceTokenParamsWithContext creates a new FindConfigReplaceTokenParams object
// with the default values initialized, and the ability to set a context for a request
func NewFindConfigReplaceTokenParamsWithContext(ctx context.Context) *FindConfigReplaceTokenParams {

	return &FindConfigReplaceTokenParams{

		Context: ctx,
	}
}

// NewFindConfigReplaceTokenParamsWithHTTPClient creates a new FindConfigReplaceTokenParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewFindConfigReplaceTokenParamsWithHTTPClient(client *http.Client) *FindConfigReplaceTokenParams {

	return &FindConfigReplaceTokenParams{
		HTTPClient: client,
	}
}

/*
FindConfigReplaceTokenParams contains all the parameters to send to the API endpoint
for the find config replace token operation typically these are written to a http.Request
*/
type FindConfigReplaceTokenParams struct {
	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the find config replace token params
func (o *FindConfigReplaceTokenParams) WithTimeout(timeout time.Duration) *FindConfigReplaceTokenParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the find config replace token params
func (o *FindConfigReplaceTokenParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the find config replace token params
func (o *FindConfigReplaceTokenParams) WithContext(ctx context.Context) *FindConfigReplaceTokenParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the find config replace token params
func (o *FindConfigReplaceTokenParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the find config replace token params
func (o *FindConfigReplaceTokenParams) WithHTTPClient(client *http.Client) *FindConfigReplaceTokenParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the find config replace token params
func (o *FindConfigReplaceTokenParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WriteToRequest writes these params to a swagger request
func (o *FindConfigReplaceTokenParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
