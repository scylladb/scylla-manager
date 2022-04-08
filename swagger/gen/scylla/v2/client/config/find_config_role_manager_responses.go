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

// FindConfigRoleManagerReader is a Reader for the FindConfigRoleManager structure.
type FindConfigRoleManagerReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *FindConfigRoleManagerReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewFindConfigRoleManagerOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewFindConfigRoleManagerDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewFindConfigRoleManagerOK creates a FindConfigRoleManagerOK with default headers values
func NewFindConfigRoleManagerOK() *FindConfigRoleManagerOK {
	return &FindConfigRoleManagerOK{}
}

/*FindConfigRoleManagerOK handles this case with default header values.

Config value
*/
type FindConfigRoleManagerOK struct {
	Payload string
}

func (o *FindConfigRoleManagerOK) GetPayload() string {
	return o.Payload
}

func (o *FindConfigRoleManagerOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewFindConfigRoleManagerDefault creates a FindConfigRoleManagerDefault with default headers values
func NewFindConfigRoleManagerDefault(code int) *FindConfigRoleManagerDefault {
	return &FindConfigRoleManagerDefault{
		_statusCode: code,
	}
}

/*FindConfigRoleManagerDefault handles this case with default header values.

unexpected error
*/
type FindConfigRoleManagerDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the find config role manager default response
func (o *FindConfigRoleManagerDefault) Code() int {
	return o._statusCode
}

func (o *FindConfigRoleManagerDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *FindConfigRoleManagerDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *FindConfigRoleManagerDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
