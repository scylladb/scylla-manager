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

// StorageServiceBackupPostReader is a Reader for the StorageServiceBackupPost structure.
type StorageServiceBackupPostReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *StorageServiceBackupPostReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewStorageServiceBackupPostOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewStorageServiceBackupPostDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewStorageServiceBackupPostOK creates a StorageServiceBackupPostOK with default headers values
func NewStorageServiceBackupPostOK() *StorageServiceBackupPostOK {
	return &StorageServiceBackupPostOK{}
}

/*
StorageServiceBackupPostOK handles this case with default header values.

Task ID that can be used with Task Manager service
*/
type StorageServiceBackupPostOK struct {
	Payload string
}

func (o *StorageServiceBackupPostOK) GetPayload() string {
	return o.Payload
}

func (o *StorageServiceBackupPostOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewStorageServiceBackupPostDefault creates a StorageServiceBackupPostDefault with default headers values
func NewStorageServiceBackupPostDefault(code int) *StorageServiceBackupPostDefault {
	return &StorageServiceBackupPostDefault{
		_statusCode: code,
	}
}

/*
StorageServiceBackupPostDefault handles this case with default header values.

internal server error
*/
type StorageServiceBackupPostDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the storage service backup post default response
func (o *StorageServiceBackupPostDefault) Code() int {
	return o._statusCode
}

func (o *StorageServiceBackupPostDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *StorageServiceBackupPostDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *StorageServiceBackupPostDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
