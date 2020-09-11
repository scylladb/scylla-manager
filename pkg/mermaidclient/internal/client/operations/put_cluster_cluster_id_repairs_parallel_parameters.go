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
	"github.com/go-openapi/swag"

	strfmt "github.com/go-openapi/strfmt"
)

// NewPutClusterClusterIDRepairsParallelParams creates a new PutClusterClusterIDRepairsParallelParams object
// with the default values initialized.
func NewPutClusterClusterIDRepairsParallelParams() *PutClusterClusterIDRepairsParallelParams {
	var ()
	return &PutClusterClusterIDRepairsParallelParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewPutClusterClusterIDRepairsParallelParamsWithTimeout creates a new PutClusterClusterIDRepairsParallelParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewPutClusterClusterIDRepairsParallelParamsWithTimeout(timeout time.Duration) *PutClusterClusterIDRepairsParallelParams {
	var ()
	return &PutClusterClusterIDRepairsParallelParams{

		timeout: timeout,
	}
}

// NewPutClusterClusterIDRepairsParallelParamsWithContext creates a new PutClusterClusterIDRepairsParallelParams object
// with the default values initialized, and the ability to set a context for a request
func NewPutClusterClusterIDRepairsParallelParamsWithContext(ctx context.Context) *PutClusterClusterIDRepairsParallelParams {
	var ()
	return &PutClusterClusterIDRepairsParallelParams{

		Context: ctx,
	}
}

// NewPutClusterClusterIDRepairsParallelParamsWithHTTPClient creates a new PutClusterClusterIDRepairsParallelParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewPutClusterClusterIDRepairsParallelParamsWithHTTPClient(client *http.Client) *PutClusterClusterIDRepairsParallelParams {
	var ()
	return &PutClusterClusterIDRepairsParallelParams{
		HTTPClient: client,
	}
}

/*PutClusterClusterIDRepairsParallelParams contains all the parameters to send to the API endpoint
for the put cluster cluster ID repairs parallel operation typically these are written to a http.Request
*/
type PutClusterClusterIDRepairsParallelParams struct {

	/*ClusterID*/
	ClusterID string
	/*Parallel*/
	Parallel int64

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the put cluster cluster ID repairs parallel params
func (o *PutClusterClusterIDRepairsParallelParams) WithTimeout(timeout time.Duration) *PutClusterClusterIDRepairsParallelParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the put cluster cluster ID repairs parallel params
func (o *PutClusterClusterIDRepairsParallelParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the put cluster cluster ID repairs parallel params
func (o *PutClusterClusterIDRepairsParallelParams) WithContext(ctx context.Context) *PutClusterClusterIDRepairsParallelParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the put cluster cluster ID repairs parallel params
func (o *PutClusterClusterIDRepairsParallelParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the put cluster cluster ID repairs parallel params
func (o *PutClusterClusterIDRepairsParallelParams) WithHTTPClient(client *http.Client) *PutClusterClusterIDRepairsParallelParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the put cluster cluster ID repairs parallel params
func (o *PutClusterClusterIDRepairsParallelParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithClusterID adds the clusterID to the put cluster cluster ID repairs parallel params
func (o *PutClusterClusterIDRepairsParallelParams) WithClusterID(clusterID string) *PutClusterClusterIDRepairsParallelParams {
	o.SetClusterID(clusterID)
	return o
}

// SetClusterID adds the clusterId to the put cluster cluster ID repairs parallel params
func (o *PutClusterClusterIDRepairsParallelParams) SetClusterID(clusterID string) {
	o.ClusterID = clusterID
}

// WithParallel adds the parallel to the put cluster cluster ID repairs parallel params
func (o *PutClusterClusterIDRepairsParallelParams) WithParallel(parallel int64) *PutClusterClusterIDRepairsParallelParams {
	o.SetParallel(parallel)
	return o
}

// SetParallel adds the parallel to the put cluster cluster ID repairs parallel params
func (o *PutClusterClusterIDRepairsParallelParams) SetParallel(parallel int64) {
	o.Parallel = parallel
}

// WriteToRequest writes these params to a swagger request
func (o *PutClusterClusterIDRepairsParallelParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	// path param cluster_id
	if err := r.SetPathParam("cluster_id", o.ClusterID); err != nil {
		return err
	}

	// query param parallel
	qrParallel := o.Parallel
	qParallel := swag.FormatInt64(qrParallel)
	if qParallel != "" {
		if err := r.SetQueryParam("parallel", qParallel); err != nil {
			return err
		}
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
