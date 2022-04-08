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

// FindConfigCompactionThroughputMbPerSecReader is a Reader for the FindConfigCompactionThroughputMbPerSec structure.
type FindConfigCompactionThroughputMbPerSecReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *FindConfigCompactionThroughputMbPerSecReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewFindConfigCompactionThroughputMbPerSecOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewFindConfigCompactionThroughputMbPerSecDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewFindConfigCompactionThroughputMbPerSecOK creates a FindConfigCompactionThroughputMbPerSecOK with default headers values
func NewFindConfigCompactionThroughputMbPerSecOK() *FindConfigCompactionThroughputMbPerSecOK {
	return &FindConfigCompactionThroughputMbPerSecOK{}
}

/*FindConfigCompactionThroughputMbPerSecOK handles this case with default header values.

Config value
*/
type FindConfigCompactionThroughputMbPerSecOK struct {
	Payload int64
}

func (o *FindConfigCompactionThroughputMbPerSecOK) GetPayload() int64 {
	return o.Payload
}

func (o *FindConfigCompactionThroughputMbPerSecOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewFindConfigCompactionThroughputMbPerSecDefault creates a FindConfigCompactionThroughputMbPerSecDefault with default headers values
func NewFindConfigCompactionThroughputMbPerSecDefault(code int) *FindConfigCompactionThroughputMbPerSecDefault {
	return &FindConfigCompactionThroughputMbPerSecDefault{
		_statusCode: code,
	}
}

/*FindConfigCompactionThroughputMbPerSecDefault handles this case with default header values.

unexpected error
*/
type FindConfigCompactionThroughputMbPerSecDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the find config compaction throughput mb per sec default response
func (o *FindConfigCompactionThroughputMbPerSecDefault) Code() int {
	return o._statusCode
}

func (o *FindConfigCompactionThroughputMbPerSecDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *FindConfigCompactionThroughputMbPerSecDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *FindConfigCompactionThroughputMbPerSecDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
