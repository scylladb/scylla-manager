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
	"github.com/go-openapi/swag"
)

// NewColumnFamilyMajorCompactionByNamePostParams creates a new ColumnFamilyMajorCompactionByNamePostParams object
// with the default values initialized.
func NewColumnFamilyMajorCompactionByNamePostParams() *ColumnFamilyMajorCompactionByNamePostParams {
	var ()
	return &ColumnFamilyMajorCompactionByNamePostParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewColumnFamilyMajorCompactionByNamePostParamsWithTimeout creates a new ColumnFamilyMajorCompactionByNamePostParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewColumnFamilyMajorCompactionByNamePostParamsWithTimeout(timeout time.Duration) *ColumnFamilyMajorCompactionByNamePostParams {
	var ()
	return &ColumnFamilyMajorCompactionByNamePostParams{

		timeout: timeout,
	}
}

// NewColumnFamilyMajorCompactionByNamePostParamsWithContext creates a new ColumnFamilyMajorCompactionByNamePostParams object
// with the default values initialized, and the ability to set a context for a request
func NewColumnFamilyMajorCompactionByNamePostParamsWithContext(ctx context.Context) *ColumnFamilyMajorCompactionByNamePostParams {
	var ()
	return &ColumnFamilyMajorCompactionByNamePostParams{

		Context: ctx,
	}
}

// NewColumnFamilyMajorCompactionByNamePostParamsWithHTTPClient creates a new ColumnFamilyMajorCompactionByNamePostParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewColumnFamilyMajorCompactionByNamePostParamsWithHTTPClient(client *http.Client) *ColumnFamilyMajorCompactionByNamePostParams {
	var ()
	return &ColumnFamilyMajorCompactionByNamePostParams{
		HTTPClient: client,
	}
}

/*
ColumnFamilyMajorCompactionByNamePostParams contains all the parameters to send to the API endpoint
for the column family major compaction by name post operation typically these are written to a http.Request
*/
type ColumnFamilyMajorCompactionByNamePostParams struct {

	/*Name
	  The column family name in keyspace:name format

	*/
	Name string
	/*SplitOutput
	  true if the output of the major compaction should be split in several sstables

	*/
	SplitOutput *bool

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the column family major compaction by name post params
func (o *ColumnFamilyMajorCompactionByNamePostParams) WithTimeout(timeout time.Duration) *ColumnFamilyMajorCompactionByNamePostParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the column family major compaction by name post params
func (o *ColumnFamilyMajorCompactionByNamePostParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the column family major compaction by name post params
func (o *ColumnFamilyMajorCompactionByNamePostParams) WithContext(ctx context.Context) *ColumnFamilyMajorCompactionByNamePostParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the column family major compaction by name post params
func (o *ColumnFamilyMajorCompactionByNamePostParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the column family major compaction by name post params
func (o *ColumnFamilyMajorCompactionByNamePostParams) WithHTTPClient(client *http.Client) *ColumnFamilyMajorCompactionByNamePostParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the column family major compaction by name post params
func (o *ColumnFamilyMajorCompactionByNamePostParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithName adds the name to the column family major compaction by name post params
func (o *ColumnFamilyMajorCompactionByNamePostParams) WithName(name string) *ColumnFamilyMajorCompactionByNamePostParams {
	o.SetName(name)
	return o
}

// SetName adds the name to the column family major compaction by name post params
func (o *ColumnFamilyMajorCompactionByNamePostParams) SetName(name string) {
	o.Name = name
}

// WithSplitOutput adds the splitOutput to the column family major compaction by name post params
func (o *ColumnFamilyMajorCompactionByNamePostParams) WithSplitOutput(splitOutput *bool) *ColumnFamilyMajorCompactionByNamePostParams {
	o.SetSplitOutput(splitOutput)
	return o
}

// SetSplitOutput adds the splitOutput to the column family major compaction by name post params
func (o *ColumnFamilyMajorCompactionByNamePostParams) SetSplitOutput(splitOutput *bool) {
	o.SplitOutput = splitOutput
}

// WriteToRequest writes these params to a swagger request
func (o *ColumnFamilyMajorCompactionByNamePostParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	// path param name
	if err := r.SetPathParam("name", o.Name); err != nil {
		return err
	}

	if o.SplitOutput != nil {

		// query param split_output
		var qrSplitOutput bool
		if o.SplitOutput != nil {
			qrSplitOutput = *o.SplitOutput
		}
		qSplitOutput := swag.FormatBool(qrSplitOutput)
		if qSplitOutput != "" {
			if err := r.SetQueryParam("split_output", qSplitOutput); err != nil {
				return err
			}
		}

	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
