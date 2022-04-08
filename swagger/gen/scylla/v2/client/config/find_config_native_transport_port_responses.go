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

// FindConfigNativeTransportPortReader is a Reader for the FindConfigNativeTransportPort structure.
type FindConfigNativeTransportPortReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *FindConfigNativeTransportPortReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewFindConfigNativeTransportPortOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewFindConfigNativeTransportPortDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewFindConfigNativeTransportPortOK creates a FindConfigNativeTransportPortOK with default headers values
func NewFindConfigNativeTransportPortOK() *FindConfigNativeTransportPortOK {
	return &FindConfigNativeTransportPortOK{}
}

/*FindConfigNativeTransportPortOK handles this case with default header values.

Config value
*/
type FindConfigNativeTransportPortOK struct {
	Payload int64
}

func (o *FindConfigNativeTransportPortOK) GetPayload() int64 {
	return o.Payload
}

func (o *FindConfigNativeTransportPortOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewFindConfigNativeTransportPortDefault creates a FindConfigNativeTransportPortDefault with default headers values
func NewFindConfigNativeTransportPortDefault(code int) *FindConfigNativeTransportPortDefault {
	return &FindConfigNativeTransportPortDefault{
		_statusCode: code,
	}
}

/*FindConfigNativeTransportPortDefault handles this case with default header values.

unexpected error
*/
type FindConfigNativeTransportPortDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the find config native transport port default response
func (o *FindConfigNativeTransportPortDefault) Code() int {
	return o._statusCode
}

func (o *FindConfigNativeTransportPortDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *FindConfigNativeTransportPortDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *FindConfigNativeTransportPortDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
