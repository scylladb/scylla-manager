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

// StorageServiceRepairAsyncByKeyspaceGetReader is a Reader for the StorageServiceRepairAsyncByKeyspaceGet structure.
type StorageServiceRepairAsyncByKeyspaceGetReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *StorageServiceRepairAsyncByKeyspaceGetReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewStorageServiceRepairAsyncByKeyspaceGetOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewStorageServiceRepairAsyncByKeyspaceGetDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewStorageServiceRepairAsyncByKeyspaceGetOK creates a StorageServiceRepairAsyncByKeyspaceGetOK with default headers values
func NewStorageServiceRepairAsyncByKeyspaceGetOK() *StorageServiceRepairAsyncByKeyspaceGetOK {
	return &StorageServiceRepairAsyncByKeyspaceGetOK{}
}

/*StorageServiceRepairAsyncByKeyspaceGetOK handles this case with default header values.

StorageServiceRepairAsyncByKeyspaceGetOK storage service repair async by keyspace get o k
*/
type StorageServiceRepairAsyncByKeyspaceGetOK struct {
	Payload models.RepairAsyncStatusResponse
}

func (o *StorageServiceRepairAsyncByKeyspaceGetOK) GetPayload() models.RepairAsyncStatusResponse {
	return o.Payload
}

func (o *StorageServiceRepairAsyncByKeyspaceGetOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewStorageServiceRepairAsyncByKeyspaceGetDefault creates a StorageServiceRepairAsyncByKeyspaceGetDefault with default headers values
func NewStorageServiceRepairAsyncByKeyspaceGetDefault(code int) *StorageServiceRepairAsyncByKeyspaceGetDefault {
	return &StorageServiceRepairAsyncByKeyspaceGetDefault{
		_statusCode: code,
	}
}

/*StorageServiceRepairAsyncByKeyspaceGetDefault handles this case with default header values.

internal server error
*/
type StorageServiceRepairAsyncByKeyspaceGetDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the storage service repair async by keyspace get default response
func (o *StorageServiceRepairAsyncByKeyspaceGetDefault) Code() int {
	return o._statusCode
}

func (o *StorageServiceRepairAsyncByKeyspaceGetDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *StorageServiceRepairAsyncByKeyspaceGetDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *StorageServiceRepairAsyncByKeyspaceGetDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
