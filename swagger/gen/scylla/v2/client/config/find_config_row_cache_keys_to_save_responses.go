// Code generated by go-swagger; DO NOT EDIT.

package config

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"
	"strings"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"

	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v2/models"
)

// FindConfigRowCacheKeysToSaveReader is a Reader for the FindConfigRowCacheKeysToSave structure.
type FindConfigRowCacheKeysToSaveReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *FindConfigRowCacheKeysToSaveReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewFindConfigRowCacheKeysToSaveOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewFindConfigRowCacheKeysToSaveDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewFindConfigRowCacheKeysToSaveOK creates a FindConfigRowCacheKeysToSaveOK with default headers values
func NewFindConfigRowCacheKeysToSaveOK() *FindConfigRowCacheKeysToSaveOK {
	return &FindConfigRowCacheKeysToSaveOK{}
}

/*FindConfigRowCacheKeysToSaveOK handles this case with default header values.

Config value
*/
type FindConfigRowCacheKeysToSaveOK struct {
	Payload int64
}

func (o *FindConfigRowCacheKeysToSaveOK) GetPayload() int64 {
	return o.Payload
}

func (o *FindConfigRowCacheKeysToSaveOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewFindConfigRowCacheKeysToSaveDefault creates a FindConfigRowCacheKeysToSaveDefault with default headers values
func NewFindConfigRowCacheKeysToSaveDefault(code int) *FindConfigRowCacheKeysToSaveDefault {
	return &FindConfigRowCacheKeysToSaveDefault{
		_statusCode: code,
	}
}

/*FindConfigRowCacheKeysToSaveDefault handles this case with default header values.

unexpected error
*/
type FindConfigRowCacheKeysToSaveDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the find config row cache keys to save default response
func (o *FindConfigRowCacheKeysToSaveDefault) Code() int {
	return o._statusCode
}

func (o *FindConfigRowCacheKeysToSaveDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *FindConfigRowCacheKeysToSaveDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *FindConfigRowCacheKeysToSaveDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
