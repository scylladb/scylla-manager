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

// FindConfigInMemoryCompactionLimitInMbReader is a Reader for the FindConfigInMemoryCompactionLimitInMb structure.
type FindConfigInMemoryCompactionLimitInMbReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *FindConfigInMemoryCompactionLimitInMbReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewFindConfigInMemoryCompactionLimitInMbOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewFindConfigInMemoryCompactionLimitInMbDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewFindConfigInMemoryCompactionLimitInMbOK creates a FindConfigInMemoryCompactionLimitInMbOK with default headers values
func NewFindConfigInMemoryCompactionLimitInMbOK() *FindConfigInMemoryCompactionLimitInMbOK {
	return &FindConfigInMemoryCompactionLimitInMbOK{}
}

/*FindConfigInMemoryCompactionLimitInMbOK handles this case with default header values.

Config value
*/
type FindConfigInMemoryCompactionLimitInMbOK struct {
	Payload int64
}

func (o *FindConfigInMemoryCompactionLimitInMbOK) GetPayload() int64 {
	return o.Payload
}

func (o *FindConfigInMemoryCompactionLimitInMbOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewFindConfigInMemoryCompactionLimitInMbDefault creates a FindConfigInMemoryCompactionLimitInMbDefault with default headers values
func NewFindConfigInMemoryCompactionLimitInMbDefault(code int) *FindConfigInMemoryCompactionLimitInMbDefault {
	return &FindConfigInMemoryCompactionLimitInMbDefault{
		_statusCode: code,
	}
}

/*FindConfigInMemoryCompactionLimitInMbDefault handles this case with default header values.

unexpected error
*/
type FindConfigInMemoryCompactionLimitInMbDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the find config in memory compaction limit in mb default response
func (o *FindConfigInMemoryCompactionLimitInMbDefault) Code() int {
	return o._statusCode
}

func (o *FindConfigInMemoryCompactionLimitInMbDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *FindConfigInMemoryCompactionLimitInMbDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *FindConfigInMemoryCompactionLimitInMbDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
