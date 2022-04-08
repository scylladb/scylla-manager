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

// FindConfigEnableDangerousDirectImportOfCassandraCountersReader is a Reader for the FindConfigEnableDangerousDirectImportOfCassandraCounters structure.
type FindConfigEnableDangerousDirectImportOfCassandraCountersReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *FindConfigEnableDangerousDirectImportOfCassandraCountersReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewFindConfigEnableDangerousDirectImportOfCassandraCountersOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewFindConfigEnableDangerousDirectImportOfCassandraCountersDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewFindConfigEnableDangerousDirectImportOfCassandraCountersOK creates a FindConfigEnableDangerousDirectImportOfCassandraCountersOK with default headers values
func NewFindConfigEnableDangerousDirectImportOfCassandraCountersOK() *FindConfigEnableDangerousDirectImportOfCassandraCountersOK {
	return &FindConfigEnableDangerousDirectImportOfCassandraCountersOK{}
}

/*FindConfigEnableDangerousDirectImportOfCassandraCountersOK handles this case with default header values.

Config value
*/
type FindConfigEnableDangerousDirectImportOfCassandraCountersOK struct {
	Payload bool
}

func (o *FindConfigEnableDangerousDirectImportOfCassandraCountersOK) GetPayload() bool {
	return o.Payload
}

func (o *FindConfigEnableDangerousDirectImportOfCassandraCountersOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewFindConfigEnableDangerousDirectImportOfCassandraCountersDefault creates a FindConfigEnableDangerousDirectImportOfCassandraCountersDefault with default headers values
func NewFindConfigEnableDangerousDirectImportOfCassandraCountersDefault(code int) *FindConfigEnableDangerousDirectImportOfCassandraCountersDefault {
	return &FindConfigEnableDangerousDirectImportOfCassandraCountersDefault{
		_statusCode: code,
	}
}

/*FindConfigEnableDangerousDirectImportOfCassandraCountersDefault handles this case with default header values.

unexpected error
*/
type FindConfigEnableDangerousDirectImportOfCassandraCountersDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the find config enable dangerous direct import of cassandra counters default response
func (o *FindConfigEnableDangerousDirectImportOfCassandraCountersDefault) Code() int {
	return o._statusCode
}

func (o *FindConfigEnableDangerousDirectImportOfCassandraCountersDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *FindConfigEnableDangerousDirectImportOfCassandraCountersDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *FindConfigEnableDangerousDirectImportOfCassandraCountersDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
