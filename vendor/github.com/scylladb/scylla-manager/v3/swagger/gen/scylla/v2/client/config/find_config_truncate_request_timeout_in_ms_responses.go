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

// FindConfigTruncateRequestTimeoutInMsReader is a Reader for the FindConfigTruncateRequestTimeoutInMs structure.
type FindConfigTruncateRequestTimeoutInMsReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *FindConfigTruncateRequestTimeoutInMsReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewFindConfigTruncateRequestTimeoutInMsOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewFindConfigTruncateRequestTimeoutInMsDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewFindConfigTruncateRequestTimeoutInMsOK creates a FindConfigTruncateRequestTimeoutInMsOK with default headers values
func NewFindConfigTruncateRequestTimeoutInMsOK() *FindConfigTruncateRequestTimeoutInMsOK {
	return &FindConfigTruncateRequestTimeoutInMsOK{}
}

/*
FindConfigTruncateRequestTimeoutInMsOK handles this case with default header values.

Config value
*/
type FindConfigTruncateRequestTimeoutInMsOK struct {
	Payload int64
}

func (o *FindConfigTruncateRequestTimeoutInMsOK) GetPayload() int64 {
	return o.Payload
}

func (o *FindConfigTruncateRequestTimeoutInMsOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewFindConfigTruncateRequestTimeoutInMsDefault creates a FindConfigTruncateRequestTimeoutInMsDefault with default headers values
func NewFindConfigTruncateRequestTimeoutInMsDefault(code int) *FindConfigTruncateRequestTimeoutInMsDefault {
	return &FindConfigTruncateRequestTimeoutInMsDefault{
		_statusCode: code,
	}
}

/*
FindConfigTruncateRequestTimeoutInMsDefault handles this case with default header values.

unexpected error
*/
type FindConfigTruncateRequestTimeoutInMsDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the find config truncate request timeout in ms default response
func (o *FindConfigTruncateRequestTimeoutInMsDefault) Code() int {
	return o._statusCode
}

func (o *FindConfigTruncateRequestTimeoutInMsDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *FindConfigTruncateRequestTimeoutInMsDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *FindConfigTruncateRequestTimeoutInMsDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
