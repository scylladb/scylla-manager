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

// NewStorageServiceTabletsRepairPostParams creates a new StorageServiceTabletsRepairPostParams object
// with the default values initialized.
func NewStorageServiceTabletsRepairPostParams() *StorageServiceTabletsRepairPostParams {
	var ()
	return &StorageServiceTabletsRepairPostParams{

		timeout: cr.DefaultTimeout,
	}
}

// NewStorageServiceTabletsRepairPostParamsWithTimeout creates a new StorageServiceTabletsRepairPostParams object
// with the default values initialized, and the ability to set a timeout on a request
func NewStorageServiceTabletsRepairPostParamsWithTimeout(timeout time.Duration) *StorageServiceTabletsRepairPostParams {
	var ()
	return &StorageServiceTabletsRepairPostParams{

		timeout: timeout,
	}
}

// NewStorageServiceTabletsRepairPostParamsWithContext creates a new StorageServiceTabletsRepairPostParams object
// with the default values initialized, and the ability to set a context for a request
func NewStorageServiceTabletsRepairPostParamsWithContext(ctx context.Context) *StorageServiceTabletsRepairPostParams {
	var ()
	return &StorageServiceTabletsRepairPostParams{

		Context: ctx,
	}
}

// NewStorageServiceTabletsRepairPostParamsWithHTTPClient creates a new StorageServiceTabletsRepairPostParams object
// with the default values initialized, and the ability to set a custom HTTPClient for a request
func NewStorageServiceTabletsRepairPostParamsWithHTTPClient(client *http.Client) *StorageServiceTabletsRepairPostParams {
	var ()
	return &StorageServiceTabletsRepairPostParams{
		HTTPClient: client,
	}
}

/*
StorageServiceTabletsRepairPostParams contains all the parameters to send to the API endpoint
for the storage service tablets repair post operation typically these are written to a http.Request
*/
type StorageServiceTabletsRepairPostParams struct {

	/*Ks
	  Keyspace name to repair

	*/
	Ks string
	/*Table
	  Table name to repair

	*/
	Table string
	/*Tokens
	  Tokens owned by the tablets to repair. Multiple tokens can be provided using a comma-separated list. When set to the special word 'all', all tablets will be repaired

	*/
	Tokens string

	timeout    time.Duration
	Context    context.Context
	HTTPClient *http.Client
}

// WithTimeout adds the timeout to the storage service tablets repair post params
func (o *StorageServiceTabletsRepairPostParams) WithTimeout(timeout time.Duration) *StorageServiceTabletsRepairPostParams {
	o.SetTimeout(timeout)
	return o
}

// SetTimeout adds the timeout to the storage service tablets repair post params
func (o *StorageServiceTabletsRepairPostParams) SetTimeout(timeout time.Duration) {
	o.timeout = timeout
}

// WithContext adds the context to the storage service tablets repair post params
func (o *StorageServiceTabletsRepairPostParams) WithContext(ctx context.Context) *StorageServiceTabletsRepairPostParams {
	o.SetContext(ctx)
	return o
}

// SetContext adds the context to the storage service tablets repair post params
func (o *StorageServiceTabletsRepairPostParams) SetContext(ctx context.Context) {
	o.Context = ctx
}

// WithHTTPClient adds the HTTPClient to the storage service tablets repair post params
func (o *StorageServiceTabletsRepairPostParams) WithHTTPClient(client *http.Client) *StorageServiceTabletsRepairPostParams {
	o.SetHTTPClient(client)
	return o
}

// SetHTTPClient adds the HTTPClient to the storage service tablets repair post params
func (o *StorageServiceTabletsRepairPostParams) SetHTTPClient(client *http.Client) {
	o.HTTPClient = client
}

// WithKs adds the ks to the storage service tablets repair post params
func (o *StorageServiceTabletsRepairPostParams) WithKs(ks string) *StorageServiceTabletsRepairPostParams {
	o.SetKs(ks)
	return o
}

// SetKs adds the ks to the storage service tablets repair post params
func (o *StorageServiceTabletsRepairPostParams) SetKs(ks string) {
	o.Ks = ks
}

// WithTable adds the table to the storage service tablets repair post params
func (o *StorageServiceTabletsRepairPostParams) WithTable(table string) *StorageServiceTabletsRepairPostParams {
	o.SetTable(table)
	return o
}

// SetTable adds the table to the storage service tablets repair post params
func (o *StorageServiceTabletsRepairPostParams) SetTable(table string) {
	o.Table = table
}

// WithTokens adds the tokens to the storage service tablets repair post params
func (o *StorageServiceTabletsRepairPostParams) WithTokens(tokens string) *StorageServiceTabletsRepairPostParams {
	o.SetTokens(tokens)
	return o
}

// SetTokens adds the tokens to the storage service tablets repair post params
func (o *StorageServiceTabletsRepairPostParams) SetTokens(tokens string) {
	o.Tokens = tokens
}

// WriteToRequest writes these params to a swagger request
func (o *StorageServiceTabletsRepairPostParams) WriteToRequest(r runtime.ClientRequest, reg strfmt.Registry) error {

	if err := r.SetTimeout(o.timeout); err != nil {
		return err
	}
	var res []error

	// query param ks
	qrKs := o.Ks
	qKs := qrKs
	if qKs != "" {
		if err := r.SetQueryParam("ks", qKs); err != nil {
			return err
		}
	}

	// query param table
	qrTable := o.Table
	qTable := qrTable
	if qTable != "" {
		if err := r.SetQueryParam("table", qTable); err != nil {
			return err
		}
	}

	// query param tokens
	qrTokens := o.Tokens
	qTokens := qrTokens
	if qTokens != "" {
		if err := r.SetQueryParam("tokens", qTokens); err != nil {
			return err
		}
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}
