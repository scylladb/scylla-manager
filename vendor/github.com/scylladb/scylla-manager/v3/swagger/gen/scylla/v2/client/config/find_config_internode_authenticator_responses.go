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

// FindConfigInternodeAuthenticatorReader is a Reader for the FindConfigInternodeAuthenticator structure.
type FindConfigInternodeAuthenticatorReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *FindConfigInternodeAuthenticatorReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewFindConfigInternodeAuthenticatorOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewFindConfigInternodeAuthenticatorDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewFindConfigInternodeAuthenticatorOK creates a FindConfigInternodeAuthenticatorOK with default headers values
func NewFindConfigInternodeAuthenticatorOK() *FindConfigInternodeAuthenticatorOK {
	return &FindConfigInternodeAuthenticatorOK{}
}

/*
FindConfigInternodeAuthenticatorOK handles this case with default header values.

Config value
*/
type FindConfigInternodeAuthenticatorOK struct {
	Payload string
}

func (o *FindConfigInternodeAuthenticatorOK) GetPayload() string {
	return o.Payload
}

func (o *FindConfigInternodeAuthenticatorOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewFindConfigInternodeAuthenticatorDefault creates a FindConfigInternodeAuthenticatorDefault with default headers values
func NewFindConfigInternodeAuthenticatorDefault(code int) *FindConfigInternodeAuthenticatorDefault {
	return &FindConfigInternodeAuthenticatorDefault{
		_statusCode: code,
	}
}

/*
FindConfigInternodeAuthenticatorDefault handles this case with default header values.

unexpected error
*/
type FindConfigInternodeAuthenticatorDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the find config internode authenticator default response
func (o *FindConfigInternodeAuthenticatorDefault) Code() int {
	return o._statusCode
}

func (o *FindConfigInternodeAuthenticatorDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *FindConfigInternodeAuthenticatorDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *FindConfigInternodeAuthenticatorDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
