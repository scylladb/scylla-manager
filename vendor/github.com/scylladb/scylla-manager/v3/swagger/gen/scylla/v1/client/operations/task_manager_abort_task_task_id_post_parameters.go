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

// NewTaskManagerAbortTaskTaskIDPostParams creates a new TaskManagerAbortTaskTaskIDPostParams object
// with the default values initialized.
func NewTaskManagerAbortTaskTaskIDPostParams() *TaskManagerAbortTaskTaskIDPostParams {
	var ()
	return &TaskManagerAbortTaskTaskIDPostParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewTaskManagerAbortTaskTaskIDPostParamsWithTimeout creates a new TaskManagerAbortTaskTaskIDPostParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewTaskManagerAbortTaskTaskIDPostParamsWithTimeout(timeout time.Duration) *TaskManagerAbortTaskTaskIDPostParams {
	var ()
	return &TaskManagerAbortTaskTaskIDPostParams{

		timeout: timeout,
	}
}

// NewTaskManagerAbortTaskTaskIDPostParamsWithContext creates a new TaskManagerAbortTaskTaskIDPostParams object
// with the default values initialized, and the ability to set a context for a request
func NewTaskManagerAbortTaskTaskIDPostParamsWithContext(ctx context.Context) *TaskManagerAbortTaskTaskIDPostParams {
	var ()
	return &TaskManagerAbortTaskTaskIDPostParams{

		Context: ctx,
	}
}

// NewTaskManagerAbortTaskTaskIDPostParamsWithHTTPClient creates a new TaskManagerAbortTaskTaskIDPostParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewTaskManagerAbortTaskTaskIDPostParamsWithHTTPClient(client *http.Client) *TaskManagerAbortTaskTaskIDPostParams {
	var ()
	return &TaskManagerAbortTaskTaskIDPostParams{
		HTTPClient: client,
	}
}

/*
TaskManagerAbortTaskTaskIDPostParams contains all the parameters to send to the API endpoint
for the task manager abort task task Id post operation typically these are written to a http.Request
*/
type TaskManagerAbortTaskTaskIDPostParams struct {

	/*TaskID
	  The uuid of a task to abort; if the task is not abortable, 403 status code is returned

	*/
	TaskID string

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the task manager abort task task Id post params
func (o *TaskManagerAbortTaskTaskIDPostParams) WithTimeout(timeout time.Duration) *TaskManagerAbortTaskTaskIDPostParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the task manager abort task task Id post params
func (o *TaskManagerAbortTaskTaskIDPostParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the task manager abort task task Id post params
func (o *TaskManagerAbortTaskTaskIDPostParams) WithContext(ctx context.Context) *TaskManagerAbortTaskTaskIDPostParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the task manager abort task task Id post params
func (o *TaskManagerAbortTaskTaskIDPostParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the task manager abort task task Id post params
func (o *TaskManagerAbortTaskTaskIDPostParams) WithHTTPClient(client *http.Client) *TaskManagerAbortTaskTaskIDPostParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the task manager abort task task Id post params
func (o *TaskManagerAbortTaskTaskIDPostParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithTaskID adds the taskID to the task manager abort task task Id post params
func (o *TaskManagerAbortTaskTaskIDPostParams) WithTaskID(taskID string) *TaskManagerAbortTaskTaskIDPostParams {
	o.SetTaskID(taskID)
	return o
}

// SetTaskID adds the taskId to the task manager abort task task Id post params
func (o *TaskManagerAbortTaskTaskIDPostParams) SetTaskID(taskID string) {
	o.TaskID = taskID
}

// WriteToRequest writes these params to a swagger request
func (o *TaskManagerAbortTaskTaskIDPostParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	// path param task_id
	if err := r.SetPathParam("task_id", o.TaskID); err != nil {
		return err
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}