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

// NewColumnFamilyAutocompactionByNameGetParams creates a new ColumnFamilyAutocompactionByNameGetParams object
// with the default values initialized.
func NewColumnFamilyAutocompactionByNameGetParams() *ColumnFamilyAutocompactionByNameGetParams {
	var ()
	return &ColumnFamilyAutocompactionByNameGetParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewColumnFamilyAutocompactionByNameGetParamsWithTimeout creates a new ColumnFamilyAutocompactionByNameGetParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewColumnFamilyAutocompactionByNameGetParamsWithTimeout(timeout time.Duration) *ColumnFamilyAutocompactionByNameGetParams {
	var ()
	return &ColumnFamilyAutocompactionByNameGetParams{

		timeout: timeout,
	}
}

// NewColumnFamilyAutocompactionByNameGetParamsWithContext creates a new ColumnFamilyAutocompactionByNameGetParams object
// with the default values initialized, and the ability to set a context for a request
func NewColumnFamilyAutocompactionByNameGetParamsWithContext(ctx context.Context) *ColumnFamilyAutocompactionByNameGetParams {
	var ()
	return &ColumnFamilyAutocompactionByNameGetParams{

		Context: ctx,
	}
}

// NewColumnFamilyAutocompactionByNameGetParamsWithHTTPClient creates a new ColumnFamilyAutocompactionByNameGetParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewColumnFamilyAutocompactionByNameGetParamsWithHTTPClient(client *http.Client) *ColumnFamilyAutocompactionByNameGetParams {
	var ()
	return &ColumnFamilyAutocompactionByNameGetParams{
		HTTPClient: client,
	}
}

/*
ColumnFamilyAutocompactionByNameGetParams contains all the parameters to send to the API endpoint
for the column family autocompaction by name get operation typically these are written to a http.Request
*/
type ColumnFamilyAutocompactionByNameGetParams struct {

	/*Name
	  The table name in keyspace:name format

	*/
	Name string

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the column family autocompaction by name get params
func (o *ColumnFamilyAutocompactionByNameGetParams) WithTimeout(timeout time.Duration) *ColumnFamilyAutocompactionByNameGetParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the column family autocompaction by name get params
func (o *ColumnFamilyAutocompactionByNameGetParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the column family autocompaction by name get params
func (o *ColumnFamilyAutocompactionByNameGetParams) WithContext(ctx context.Context) *ColumnFamilyAutocompactionByNameGetParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the column family autocompaction by name get params
func (o *ColumnFamilyAutocompactionByNameGetParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the column family autocompaction by name get params
func (o *ColumnFamilyAutocompactionByNameGetParams) WithHTTPClient(client *http.Client) *ColumnFamilyAutocompactionByNameGetParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the column family autocompaction by name get params
func (o *ColumnFamilyAutocompactionByNameGetParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithName adds the name to the column family autocompaction by name get params
func (o *ColumnFamilyAutocompactionByNameGetParams) WithName(name string) *ColumnFamilyAutocompactionByNameGetParams {
	o.SetName(name)
	return o
}

// SetName adds the name to the column family autocompaction by name get params
func (o *ColumnFamilyAutocompactionByNameGetParams) SetName(name string) {
	o.Name = name
}

// WriteToRequest writes these params to a swagger request
func (o *ColumnFamilyAutocompactionByNameGetParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	// path param name
	if err := r.SetPathParam("name", o.Name); err != nil {
		return err
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
