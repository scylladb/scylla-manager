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

	"github.com/scylladb/scylla-manager/v3/swagger/gen/agent/models"
)

// NewOperationsAboutParams creates a new OperationsAboutParams object
// with the default values initialized.
func NewOperationsAboutParams() *OperationsAboutParams {
	var ()
	return &OperationsAboutParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewOperationsAboutParamsWithTimeout creates a new OperationsAboutParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewOperationsAboutParamsWithTimeout(timeout time.Duration) *OperationsAboutParams {
	var ()
	return &OperationsAboutParams{

		timeout: timeout,
	}
}

// NewOperationsAboutParamsWithContext creates a new OperationsAboutParams object
// with the default values initialized, and the ability to set a context for a request
func NewOperationsAboutParamsWithContext(ctx context.Context) *OperationsAboutParams {
	var ()
	return &OperationsAboutParams{

		Context: ctx,
	}
}

// NewOperationsAboutParamsWithHTTPClient creates a new OperationsAboutParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewOperationsAboutParamsWithHTTPClient(client *http.Client) *OperationsAboutParams {
	var ()
	return &OperationsAboutParams{
		HTTPClient: client,
	}
}

/*
OperationsAboutParams contains all the parameters to send to the API endpoint
for the operations about operation typically these are written to a http.Request
*/
type OperationsAboutParams struct {

	/*RemotePath
	  Remote path

	*/
	RemotePath *models.RemotePath

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the operations about params
func (o *OperationsAboutParams) WithTimeout(timeout time.Duration) *OperationsAboutParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the operations about params
func (o *OperationsAboutParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the operations about params
func (o *OperationsAboutParams) WithContext(ctx context.Context) *OperationsAboutParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the operations about params
func (o *OperationsAboutParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the operations about params
func (o *OperationsAboutParams) WithHTTPClient(client *http.Client) *OperationsAboutParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the operations about params
func (o *OperationsAboutParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithRemotePath adds the remotePath to the operations about params
func (o *OperationsAboutParams) WithRemotePath(remotePath *models.RemotePath) *OperationsAboutParams {
	o.SetRemotePath(remotePath)
	return o
}

// SetRemotePath adds the remotePath to the operations about params
func (o *OperationsAboutParams) SetRemotePath(remotePath *models.RemotePath) {
	o.RemotePath = remotePath
}

// WriteToRequest writes these params to a swagger request
func (o *OperationsAboutParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

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
