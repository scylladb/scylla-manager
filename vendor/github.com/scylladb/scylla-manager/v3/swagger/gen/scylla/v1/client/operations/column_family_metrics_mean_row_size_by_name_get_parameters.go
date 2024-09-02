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

// NewColumnFamilyMetricsMeanRowSizeByNameGetParams creates a new ColumnFamilyMetricsMeanRowSizeByNameGetParams object
// with the default values initialized.
func NewColumnFamilyMetricsMeanRowSizeByNameGetParams() *ColumnFamilyMetricsMeanRowSizeByNameGetParams {
	var ()
	return &ColumnFamilyMetricsMeanRowSizeByNameGetParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewColumnFamilyMetricsMeanRowSizeByNameGetParamsWithTimeout creates a new ColumnFamilyMetricsMeanRowSizeByNameGetParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewColumnFamilyMetricsMeanRowSizeByNameGetParamsWithTimeout(timeout time.Duration) *ColumnFamilyMetricsMeanRowSizeByNameGetParams {
	var ()
	return &ColumnFamilyMetricsMeanRowSizeByNameGetParams{

		timeout: timeout,
	}
}

// NewColumnFamilyMetricsMeanRowSizeByNameGetParamsWithContext creates a new ColumnFamilyMetricsMeanRowSizeByNameGetParams object
// with the default values initialized, and the ability to set a context for a request
func NewColumnFamilyMetricsMeanRowSizeByNameGetParamsWithContext(ctx context.Context) *ColumnFamilyMetricsMeanRowSizeByNameGetParams {
	var ()
	return &ColumnFamilyMetricsMeanRowSizeByNameGetParams{

		Context: ctx,
	}
}

// NewColumnFamilyMetricsMeanRowSizeByNameGetParamsWithHTTPClient creates a new ColumnFamilyMetricsMeanRowSizeByNameGetParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewColumnFamilyMetricsMeanRowSizeByNameGetParamsWithHTTPClient(client *http.Client) *ColumnFamilyMetricsMeanRowSizeByNameGetParams {
	var ()
	return &ColumnFamilyMetricsMeanRowSizeByNameGetParams{
		HTTPClient: client,
	}
}

/*
ColumnFamilyMetricsMeanRowSizeByNameGetParams contains all the parameters to send to the API endpoint
for the column family metrics mean row size by name get operation typically these are written to a http.Request
*/
type ColumnFamilyMetricsMeanRowSizeByNameGetParams struct {

	/*Name
	  The column family name in keyspace:name format

	*/
	Name string

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the column family metrics mean row size by name get params
func (o *ColumnFamilyMetricsMeanRowSizeByNameGetParams) WithTimeout(timeout time.Duration) *ColumnFamilyMetricsMeanRowSizeByNameGetParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the column family metrics mean row size by name get params
func (o *ColumnFamilyMetricsMeanRowSizeByNameGetParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the column family metrics mean row size by name get params
func (o *ColumnFamilyMetricsMeanRowSizeByNameGetParams) WithContext(ctx context.Context) *ColumnFamilyMetricsMeanRowSizeByNameGetParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the column family metrics mean row size by name get params
func (o *ColumnFamilyMetricsMeanRowSizeByNameGetParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the column family metrics mean row size by name get params
func (o *ColumnFamilyMetricsMeanRowSizeByNameGetParams) WithHTTPClient(client *http.Client) *ColumnFamilyMetricsMeanRowSizeByNameGetParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the column family metrics mean row size by name get params
func (o *ColumnFamilyMetricsMeanRowSizeByNameGetParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithName adds the name to the column family metrics mean row size by name get params
func (o *ColumnFamilyMetricsMeanRowSizeByNameGetParams) WithName(name string) *ColumnFamilyMetricsMeanRowSizeByNameGetParams {
	o.SetName(name)
	return o
}

// SetName adds the name to the column family metrics mean row size by name get params
func (o *ColumnFamilyMetricsMeanRowSizeByNameGetParams) SetName(name string) {
	o.Name = name
}

// WriteToRequest writes these params to a swagger request
func (o *ColumnFamilyMetricsMeanRowSizeByNameGetParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

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
