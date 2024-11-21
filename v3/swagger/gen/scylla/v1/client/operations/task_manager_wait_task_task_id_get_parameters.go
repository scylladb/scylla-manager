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

// NewTaskManagerWaitTaskTaskIDGetParams creates a new TaskManagerWaitTaskTaskIDGetParams object
// with the default values initialized.
func NewTaskManagerWaitTaskTaskIDGetParams() *TaskManagerWaitTaskTaskIDGetParams {
	var ()
	return &TaskManagerWaitTaskTaskIDGetParams{

		requestTimeout: cr.DefaultTimeout,
	}
}

// NewTaskManagerWaitTaskTaskIDGetParamsWithTimeout creates a new TaskManagerWaitTaskTaskIDGetParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewTaskManagerWaitTaskTaskIDGetParamsWithTimeout(timeout time.Duration) *TaskManagerWaitTaskTaskIDGetParams {
	var ()
	return &TaskManagerWaitTaskTaskIDGetParams{

		requestTimeout: timeout,
	}
}

// NewTaskManagerWaitTaskTaskIDGetParamsWithContext creates a new TaskManagerWaitTaskTaskIDGetParams object
// with the default values initialized, and the ability to set a context for a request
func NewTaskManagerWaitTaskTaskIDGetParamsWithContext(ctx context.Context) *TaskManagerWaitTaskTaskIDGetParams {
	var ()
	return &TaskManagerWaitTaskTaskIDGetParams{

		Context: ctx,
	}
}

// NewTaskManagerWaitTaskTaskIDGetParamsWithHTTPClient creates a new TaskManagerWaitTaskTaskIDGetParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewTaskManagerWaitTaskTaskIDGetParamsWithHTTPClient(client *http.Client) *TaskManagerWaitTaskTaskIDGetParams {
	var ()
	return &TaskManagerWaitTaskTaskIDGetParams{
		HTTPClient: client,
	}
}

/*
TaskManagerWaitTaskTaskIDGetParams contains all the parameters to send to the API endpoint
for the task manager wait task task Id get operation typically these are written to a http.Request
*/
type TaskManagerWaitTaskTaskIDGetParams struct {

	/*TaskID
	  The uuid of a task to wait for

	*/
	TaskID string
	/*Timeout
	  Timeout for waiting; if times out, 408 status code is returned

	*/
	Timeout *int64

	requestTimeout time.Duration
	Context        context.Context
	HTTPClient     *http.Client
}

// WithRequestTimeout adds the timeout to the task manager wait task task Id get params
func (o *TaskManagerWaitTaskTaskIDGetParams) WithRequestTimeout(timeout time.Duration) *TaskManagerWaitTaskTaskIDGetParams {
	o.SetRequestTimeout(timeout)
	return o
}

// SetRequestTimeout adds the timeout to the task manager wait task task Id get params
func (o *TaskManagerWaitTaskTaskIDGetParams) SetRequestTimeout(timeout time.Duration) {
	o.requestTimeout = timeout
}

// WithContext adds the context to the task manager wait task task Id get params
func (o *TaskManagerWaitTaskTaskIDGetParams) WithContext(ctx context.Context) *TaskManagerWaitTaskTaskIDGetParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the task manager wait task task Id get params
func (o *TaskManagerWaitTaskTaskIDGetParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the task manager wait task task Id get params
func (o *TaskManagerWaitTaskTaskIDGetParams) WithHTTPClient(client *http.Client) *TaskManagerWaitTaskTaskIDGetParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the task manager wait task task Id get params
func (o *TaskManagerWaitTaskTaskIDGetParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithTaskID adds the taskID to the task manager wait task task Id get params
func (o *TaskManagerWaitTaskTaskIDGetParams) WithTaskID(taskID string) *TaskManagerWaitTaskTaskIDGetParams {
	o.SetTaskID(taskID)
	return o
}

// SetTaskID adds the taskId to the task manager wait task task Id get params
func (o *TaskManagerWaitTaskTaskIDGetParams) SetTaskID(taskID string) {
	o.TaskID = taskID
}

// WithTimeout adds the timeout to the task manager wait task task Id get params
func (o *TaskManagerWaitTaskTaskIDGetParams) WithTimeout(timeout *int64) *TaskManagerWaitTaskTaskIDGetParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the task manager wait task task Id get params
func (o *TaskManagerWaitTaskTaskIDGetParams) SetTimeout(timeout *int64) {
	o.Timeout = timeout
}

// WriteToRequest writes these params to a swagger request
func (o *TaskManagerWaitTaskTaskIDGetParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.requestTimeout); err != nil {
		return err
	}
	var res []error

	// path param task_id
	if err := r.SetPathParam("task_id", o.TaskID); err != nil {
		return err
	}

	if o.Timeout != nil {

		// query param timeout
		var qrTimeout int64
		if o.Timeout != nil {
			qrTimeout = *o.Timeout
		}
		qTimeout := swag.FormatInt64(qrTimeout)
		if qTimeout != "" {
			if err := r.SetQueryParam("timeout", qTimeout); err != nil {
				return err
			}
		}

	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}