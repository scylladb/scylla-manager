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

// NewColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams creates a new ColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams object
// with the default values initialized.
func NewColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams() *ColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams {
	var ()
	return &ColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParamsWithTimeout creates a new ColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParamsWithTimeout(timeout time.Duration) *ColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams {
	var ()
	return &ColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams{

		timeout: timeout,
	}
}

// NewColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParamsWithContext creates a new ColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams object
// with the default values initialized, and the ability to set a context for a request
func NewColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParamsWithContext(ctx context.Context) *ColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams {
	var ()
	return &ColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams{

		Context: ctx,
	}
}

// NewColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParamsWithHTTPClient creates a new ColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParamsWithHTTPClient(client *http.Client) *ColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams {
	var ()
	return &ColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams{
		HTTPClient: client,
	}
}

/*
ColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams contains all the parameters to send to the API endpoint
for the column family metrics cas commit estimated histogram by name get operation typically these are written to a http.Request
*/
type ColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams struct {

	/*Name
	  The column family name in keyspace:name format

	*/
	Name string

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the column family metrics cas commit estimated histogram by name get params
func (o *ColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams) WithTimeout(timeout time.Duration) *ColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the column family metrics cas commit estimated histogram by name get params
func (o *ColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the column family metrics cas commit estimated histogram by name get params
func (o *ColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams) WithContext(ctx context.Context) *ColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the column family metrics cas commit estimated histogram by name get params
func (o *ColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the column family metrics cas commit estimated histogram by name get params
func (o *ColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams) WithHTTPClient(client *http.Client) *ColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the column family metrics cas commit estimated histogram by name get params
func (o *ColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithName adds the name to the column family metrics cas commit estimated histogram by name get params
func (o *ColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams) WithName(name string) *ColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams {
	o.SetName(name)
	return o
}

// SetName adds the name to the column family metrics cas commit estimated histogram by name get params
func (o *ColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams) SetName(name string) {
	o.Name = name
}

// WriteToRequest writes these params to a swagger request
func (o *ColumnFamilyMetricsCasCommitEstimatedHistogramByNameGetParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

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
