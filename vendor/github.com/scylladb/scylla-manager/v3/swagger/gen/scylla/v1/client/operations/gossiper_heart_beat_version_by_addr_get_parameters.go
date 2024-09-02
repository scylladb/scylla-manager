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

// NewGossiperHeartBeatVersionByAddrGetParams creates a new GossiperHeartBeatVersionByAddrGetParams object
// with the default values initialized.
func NewGossiperHeartBeatVersionByAddrGetParams() *GossiperHeartBeatVersionByAddrGetParams {
	var ()
	return &GossiperHeartBeatVersionByAddrGetParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewGossiperHeartBeatVersionByAddrGetParamsWithTimeout creates a new GossiperHeartBeatVersionByAddrGetParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewGossiperHeartBeatVersionByAddrGetParamsWithTimeout(timeout time.Duration) *GossiperHeartBeatVersionByAddrGetParams {
	var ()
	return &GossiperHeartBeatVersionByAddrGetParams{

		timeout: timeout,
	}
}

// NewGossiperHeartBeatVersionByAddrGetParamsWithContext creates a new GossiperHeartBeatVersionByAddrGetParams object
// with the default values initialized, and the ability to set a context for a request
func NewGossiperHeartBeatVersionByAddrGetParamsWithContext(ctx context.Context) *GossiperHeartBeatVersionByAddrGetParams {
	var ()
	return &GossiperHeartBeatVersionByAddrGetParams{

		Context: ctx,
	}
}

// NewGossiperHeartBeatVersionByAddrGetParamsWithHTTPClient creates a new GossiperHeartBeatVersionByAddrGetParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewGossiperHeartBeatVersionByAddrGetParamsWithHTTPClient(client *http.Client) *GossiperHeartBeatVersionByAddrGetParams {
	var ()
	return &GossiperHeartBeatVersionByAddrGetParams{
		HTTPClient: client,
	}
}

/*
GossiperHeartBeatVersionByAddrGetParams contains all the parameters to send to the API endpoint
for the gossiper heart beat version by addr get operation typically these are written to a http.Request
*/
type GossiperHeartBeatVersionByAddrGetParams struct {

	/*Addr
	  The endpoint address

	*/
	Addr string

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the gossiper heart beat version by addr get params
func (o *GossiperHeartBeatVersionByAddrGetParams) WithTimeout(timeout time.Duration) *GossiperHeartBeatVersionByAddrGetParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the gossiper heart beat version by addr get params
func (o *GossiperHeartBeatVersionByAddrGetParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the gossiper heart beat version by addr get params
func (o *GossiperHeartBeatVersionByAddrGetParams) WithContext(ctx context.Context) *GossiperHeartBeatVersionByAddrGetParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the gossiper heart beat version by addr get params
func (o *GossiperHeartBeatVersionByAddrGetParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the gossiper heart beat version by addr get params
func (o *GossiperHeartBeatVersionByAddrGetParams) WithHTTPClient(client *http.Client) *GossiperHeartBeatVersionByAddrGetParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the gossiper heart beat version by addr get params
func (o *GossiperHeartBeatVersionByAddrGetParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithAddr adds the addr to the gossiper heart beat version by addr get params
func (o *GossiperHeartBeatVersionByAddrGetParams) WithAddr(addr string) *GossiperHeartBeatVersionByAddrGetParams {
	o.SetAddr(addr)
	return o
}

// SetAddr adds the addr to the gossiper heart beat version by addr get params
func (o *GossiperHeartBeatVersionByAddrGetParams) SetAddr(addr string) {
	o.Addr = addr
}

// WriteToRequest writes these params to a swagger request
func (o *GossiperHeartBeatVersionByAddrGetParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	// path param addr
	if err := r.SetPathParam("addr", o.Addr); err != nil {
		return err
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
