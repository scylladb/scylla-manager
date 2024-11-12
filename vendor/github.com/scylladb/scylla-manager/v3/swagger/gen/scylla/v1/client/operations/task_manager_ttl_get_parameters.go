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

// NewTaskManagerTTLGetParams creates a new TaskManagerTTLGetParams object
// with the default values initialized.
func NewTaskManagerTTLGetParams() *TaskManagerTTLGetParams {

	return &TaskManagerTTLGetParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewTaskManagerTTLGetParamsWithTimeout creates a new TaskManagerTTLGetParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewTaskManagerTTLGetParamsWithTimeout(timeout time.Duration) *TaskManagerTTLGetParams {

	return &TaskManagerTTLGetParams{

		timeout: timeout,
	}
}

// NewTaskManagerTTLGetParamsWithContext creates a new TaskManagerTTLGetParams object
// with the default values initialized, and the ability to set a context for a request
func NewTaskManagerTTLGetParamsWithContext(ctx context.Context) *TaskManagerTTLGetParams {

	return &TaskManagerTTLGetParams{

		Context: ctx,
	}
}

// NewTaskManagerTTLGetParamsWithHTTPClient creates a new TaskManagerTTLGetParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewTaskManagerTTLGetParamsWithHTTPClient(client *http.Client) *TaskManagerTTLGetParams {

	return &TaskManagerTTLGetParams{
		HTTPClient: client,
	}
}

/*
TaskManagerTTLGetParams contains all the parameters to send to the API endpoint
for the task manager Ttl get operation typically these are written to a http.Request
*/
type TaskManagerTTLGetParams struct {
	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the task manager Ttl get params
func (o *TaskManagerTTLGetParams) WithTimeout(timeout time.Duration) *TaskManagerTTLGetParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the task manager Ttl get params
func (o *TaskManagerTTLGetParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the task manager Ttl get params
func (o *TaskManagerTTLGetParams) WithContext(ctx context.Context) *TaskManagerTTLGetParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the task manager Ttl get params
func (o *TaskManagerTTLGetParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the task manager Ttl get params
func (o *TaskManagerTTLGetParams) WithHTTPClient(client *http.Client) *TaskManagerTTLGetParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the task manager Ttl get params
func (o *TaskManagerTTLGetParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WriteToRequest writes these params to a swagger request
func (o *TaskManagerTTLGetParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
