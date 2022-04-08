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

// FindConfigEnableCommitlogReader is a Reader for the FindConfigEnableCommitlog structure.
type FindConfigEnableCommitlogReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *FindConfigEnableCommitlogReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewFindConfigEnableCommitlogOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewFindConfigEnableCommitlogDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewFindConfigEnableCommitlogOK creates a FindConfigEnableCommitlogOK with default headers values
func NewFindConfigEnableCommitlogOK() *FindConfigEnableCommitlogOK {
	return &FindConfigEnableCommitlogOK{}
}

/*FindConfigEnableCommitlogOK handles this case with default header values.

Config value
*/
type FindConfigEnableCommitlogOK struct {
	Payload bool
}

func (o *FindConfigEnableCommitlogOK) GetPayload() bool {
	return o.Payload
}

func (o *FindConfigEnableCommitlogOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewFindConfigEnableCommitlogDefault creates a FindConfigEnableCommitlogDefault with default headers values
func NewFindConfigEnableCommitlogDefault(code int) *FindConfigEnableCommitlogDefault {
	return &FindConfigEnableCommitlogDefault{
		_statusCode: code,
	}
}

/*FindConfigEnableCommitlogDefault handles this case with default header values.

unexpected error
*/
type FindConfigEnableCommitlogDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the find config enable commitlog default response
func (o *FindConfigEnableCommitlogDefault) Code() int {
	return o._statusCode
}

func (o *FindConfigEnableCommitlogDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *FindConfigEnableCommitlogDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *FindConfigEnableCommitlogDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
