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

// StorageServiceSchemaVersionGetReader is a Reader for the StorageServiceSchemaVersionGet structure.
type StorageServiceSchemaVersionGetReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *StorageServiceSchemaVersionGetReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewStorageServiceSchemaVersionGetOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewStorageServiceSchemaVersionGetDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewStorageServiceSchemaVersionGetOK creates a StorageServiceSchemaVersionGetOK with default headers values
func NewStorageServiceSchemaVersionGetOK() *StorageServiceSchemaVersionGetOK {
	return &StorageServiceSchemaVersionGetOK{}
}

/*StorageServiceSchemaVersionGetOK handles this case with default header values.

StorageServiceSchemaVersionGetOK storage service schema version get o k
*/
type StorageServiceSchemaVersionGetOK struct {
	Payload string
}

func (o *StorageServiceSchemaVersionGetOK) GetPayload() string {
	return o.Payload
}

func (o *StorageServiceSchemaVersionGetOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewStorageServiceSchemaVersionGetDefault creates a StorageServiceSchemaVersionGetDefault with default headers values
func NewStorageServiceSchemaVersionGetDefault(code int) *StorageServiceSchemaVersionGetDefault {
	return &StorageServiceSchemaVersionGetDefault{
		_statusCode: code,
	}
}

/*StorageServiceSchemaVersionGetDefault handles this case with default header values.

internal server error
*/
type StorageServiceSchemaVersionGetDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the storage service schema version get default response
func (o *StorageServiceSchemaVersionGetDefault) Code() int {
	return o._statusCode
}

func (o *StorageServiceSchemaVersionGetDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *StorageServiceSchemaVersionGetDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *StorageServiceSchemaVersionGetDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
