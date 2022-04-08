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

// StorageServiceSnapshotsDeleteReader is a Reader for the StorageServiceSnapshotsDelete structure.
type StorageServiceSnapshotsDeleteReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *StorageServiceSnapshotsDeleteReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewStorageServiceSnapshotsDeleteOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewStorageServiceSnapshotsDeleteDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewStorageServiceSnapshotsDeleteOK creates a StorageServiceSnapshotsDeleteOK with default headers values
func NewStorageServiceSnapshotsDeleteOK() *StorageServiceSnapshotsDeleteOK {
	return &StorageServiceSnapshotsDeleteOK{}
}

/*StorageServiceSnapshotsDeleteOK handles this case with default header values.

StorageServiceSnapshotsDeleteOK storage service snapshots delete o k
*/
type StorageServiceSnapshotsDeleteOK struct {
}

func (o *StorageServiceSnapshotsDeleteOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewStorageServiceSnapshotsDeleteDefault creates a StorageServiceSnapshotsDeleteDefault with default headers values
func NewStorageServiceSnapshotsDeleteDefault(code int) *StorageServiceSnapshotsDeleteDefault {
	return &StorageServiceSnapshotsDeleteDefault{
		_statusCode: code,
	}
}

/*StorageServiceSnapshotsDeleteDefault handles this case with default header values.

internal server error
*/
type StorageServiceSnapshotsDeleteDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the storage service snapshots delete default response
func (o *StorageServiceSnapshotsDeleteDefault) Code() int {
	return o._statusCode
}

func (o *StorageServiceSnapshotsDeleteDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *StorageServiceSnapshotsDeleteDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *StorageServiceSnapshotsDeleteDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
