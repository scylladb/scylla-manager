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

// NewJobInfoParams creates a new JobInfoParams object
// with the default values initialized.
func NewJobInfoParams() *JobInfoParams {
	var ()
	return &JobInfoParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewJobInfoParamsWithTimeout creates a new JobInfoParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewJobInfoParamsWithTimeout(timeout time.Duration) *JobInfoParams {
	var ()
	return &JobInfoParams{

		timeout: timeout,
	}
}

// NewJobInfoParamsWithContext creates a new JobInfoParams object
// with the default values initialized, and the ability to set a context for a request
func NewJobInfoParamsWithContext(ctx context.Context) *JobInfoParams {
	var ()
	return &JobInfoParams{

		Context: ctx,
	}
}

// NewJobInfoParamsWithHTTPClient creates a new JobInfoParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewJobInfoParamsWithHTTPClient(client *http.Client) *JobInfoParams {
	var ()
	return &JobInfoParams{
		HTTPClient: client,
	}
}

/*JobInfoParams contains all the parameters to send to the API endpoint
for the job info operation typically these are written to a http.Request
*/
type JobInfoParams struct {

	/*Jobinfo
	  Job info params with id and long polling

	*/
	Jobinfo *models.JobInfoParams

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the job info params
func (o *JobInfoParams) WithTimeout(timeout time.Duration) *JobInfoParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the job info params
func (o *JobInfoParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the job info params
func (o *JobInfoParams) WithContext(ctx context.Context) *JobInfoParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the job info params
func (o *JobInfoParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the job info params
func (o *JobInfoParams) WithHTTPClient(client *http.Client) *JobInfoParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the job info params
func (o *JobInfoParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithJobinfo adds the jobinfo to the job info params
func (o *JobInfoParams) WithJobinfo(jobinfo *models.JobInfoParams) *JobInfoParams {
	o.SetJobinfo(jobinfo)
	return o
}

// SetJobinfo adds the jobinfo to the job info params
func (o *JobInfoParams) SetJobinfo(jobinfo *models.JobInfoParams) {
	o.Jobinfo = jobinfo
}

// WriteToRequest writes these params to a swagger request
func (o *JobInfoParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	if o.Jobinfo != nil {
		if err := r.SetBodyParam(o.Jobinfo); err != nil {
			return err
		}
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
