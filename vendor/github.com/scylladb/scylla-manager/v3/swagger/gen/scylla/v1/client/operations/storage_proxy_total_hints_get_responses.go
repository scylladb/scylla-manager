// Code generated by go-swagger; DO NOT EDIT.

package operations

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"
	"strings"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"

	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v1/models"
)

// StorageProxyTotalHintsGetReader is a Reader for the StorageProxyTotalHintsGet structure.
type StorageProxyTotalHintsGetReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *StorageProxyTotalHintsGetReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewStorageProxyTotalHintsGetOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewStorageProxyTotalHintsGetDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewStorageProxyTotalHintsGetOK creates a StorageProxyTotalHintsGetOK with default headers values
func NewStorageProxyTotalHintsGetOK() *StorageProxyTotalHintsGetOK {
	return &StorageProxyTotalHintsGetOK{}
}

/*
StorageProxyTotalHintsGetOK handles this case with default header values.

Success
*/
type StorageProxyTotalHintsGetOK struct {
	Payload interface{}
}

func (o *StorageProxyTotalHintsGetOK) GetPayload() interface{} {
	return o.Payload
}

func (o *StorageProxyTotalHintsGetOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewStorageProxyTotalHintsGetDefault creates a StorageProxyTotalHintsGetDefault with default headers values
func NewStorageProxyTotalHintsGetDefault(code int) *StorageProxyTotalHintsGetDefault {
	return &StorageProxyTotalHintsGetDefault{
		_statusCode: code,
	}
}

/*
StorageProxyTotalHintsGetDefault handles this case with default header values.

internal server error
*/
type StorageProxyTotalHintsGetDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the storage proxy total hints get default response
func (o *StorageProxyTotalHintsGetDefault) Code() int {
	return o._statusCode
}

func (o *StorageProxyTotalHintsGetDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *StorageProxyTotalHintsGetDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *StorageProxyTotalHintsGetDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
