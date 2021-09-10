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

	"github.com/scylladb/scylla-manager/swagger/gen/agent/models"
)

// NewOperationsFileInfoParams creates a new OperationsFileInfoParams object
// with the default values initialized.
func NewOperationsFileInfoParams() *OperationsFileInfoParams {
	var ()
	return &OperationsFileInfoParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewOperationsFileInfoParamsWithTimeout creates a new OperationsFileInfoParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewOperationsFileInfoParamsWithTimeout(timeout time.Duration) *OperationsFileInfoParams {
	var ()
	return &OperationsFileInfoParams{

		timeout: timeout,
	}
}

// NewOperationsFileInfoParamsWithContext creates a new OperationsFileInfoParams object
// with the default values initialized, and the ability to set a context for a request
func NewOperationsFileInfoParamsWithContext(ctx context.Context) *OperationsFileInfoParams {
	var ()
	return &OperationsFileInfoParams{

		Context: ctx,
	}
}

// NewOperationsFileInfoParamsWithHTTPClient creates a new OperationsFileInfoParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewOperationsFileInfoParamsWithHTTPClient(client *http.Client) *OperationsFileInfoParams {
	var ()
	return &OperationsFileInfoParams{
		HTTPClient: client,
	}
}

/*OperationsFileInfoParams contains all the parameters to send to the API endpoint
for the operations file info operation typically these are written to a http.Request
*/
type OperationsFileInfoParams struct {

	/*RemotePath
	  Remote path

	*/
	RemotePath *models.RemotePath

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the operations file info params
func (o *OperationsFileInfoParams) WithTimeout(timeout time.Duration) *OperationsFileInfoParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the operations file info params
func (o *OperationsFileInfoParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the operations file info params
func (o *OperationsFileInfoParams) WithContext(ctx context.Context) *OperationsFileInfoParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the operations file info params
func (o *OperationsFileInfoParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the operations file info params
func (o *OperationsFileInfoParams) WithHTTPClient(client *http.Client) *OperationsFileInfoParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the operations file info params
func (o *OperationsFileInfoParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithRemotePath adds the remotePath to the operations file info params
func (o *OperationsFileInfoParams) WithRemotePath(remotePath *models.RemotePath) *OperationsFileInfoParams {
	o.SetRemotePath(remotePath)
	return o
}

// SetRemotePath adds the remotePath to the operations file info params
func (o *OperationsFileInfoParams) SetRemotePath(remotePath *models.RemotePath) {
	o.RemotePath = remotePath
}

// WriteToRequest writes these params to a swagger request
func (o *OperationsFileInfoParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	if o.RemotePath != nil {
		if err := r.SetBodyParam(o.RemotePath); err != nil {
			return err
		}
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
