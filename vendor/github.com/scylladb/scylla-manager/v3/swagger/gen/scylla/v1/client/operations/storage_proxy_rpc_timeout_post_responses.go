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

// StorageProxyRPCTimeoutPostReader is a Reader for the StorageProxyRPCTimeoutPost structure.
type StorageProxyRPCTimeoutPostReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *StorageProxyRPCTimeoutPostReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewStorageProxyRPCTimeoutPostOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewStorageProxyRPCTimeoutPostDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewStorageProxyRPCTimeoutPostOK creates a StorageProxyRPCTimeoutPostOK with default headers values
func NewStorageProxyRPCTimeoutPostOK() *StorageProxyRPCTimeoutPostOK {
	return &StorageProxyRPCTimeoutPostOK{}
}

/*
StorageProxyRPCTimeoutPostOK handles this case with default header values.

Success
*/
type StorageProxyRPCTimeoutPostOK struct {
}

func (o *StorageProxyRPCTimeoutPostOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewStorageProxyRPCTimeoutPostDefault creates a StorageProxyRPCTimeoutPostDefault with default headers values
func NewStorageProxyRPCTimeoutPostDefault(code int) *StorageProxyRPCTimeoutPostDefault {
	return &StorageProxyRPCTimeoutPostDefault{
		_statusCode: code,
	}
}

/*
StorageProxyRPCTimeoutPostDefault handles this case with default header values.

internal server error
*/
type StorageProxyRPCTimeoutPostDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the storage proxy Rpc timeout post default response
func (o *StorageProxyRPCTimeoutPostDefault) Code() int {
	return o._statusCode
}

func (o *StorageProxyRPCTimeoutPostDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *StorageProxyRPCTimeoutPostDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *StorageProxyRPCTimeoutPostDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
