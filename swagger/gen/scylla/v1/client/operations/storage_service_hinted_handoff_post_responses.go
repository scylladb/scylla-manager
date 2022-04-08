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

// StorageServiceHintedHandoffPostReader is a Reader for the StorageServiceHintedHandoffPost structure.
type StorageServiceHintedHandoffPostReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *StorageServiceHintedHandoffPostReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewStorageServiceHintedHandoffPostOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewStorageServiceHintedHandoffPostDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewStorageServiceHintedHandoffPostOK creates a StorageServiceHintedHandoffPostOK with default headers values
func NewStorageServiceHintedHandoffPostOK() *StorageServiceHintedHandoffPostOK {
	return &StorageServiceHintedHandoffPostOK{}
}

/*StorageServiceHintedHandoffPostOK handles this case with default header values.

StorageServiceHintedHandoffPostOK storage service hinted handoff post o k
*/
type StorageServiceHintedHandoffPostOK struct {
}

func (o *StorageServiceHintedHandoffPostOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewStorageServiceHintedHandoffPostDefault creates a StorageServiceHintedHandoffPostDefault with default headers values
func NewStorageServiceHintedHandoffPostDefault(code int) *StorageServiceHintedHandoffPostDefault {
	return &StorageServiceHintedHandoffPostDefault{
		_statusCode: code,
	}
}

/*StorageServiceHintedHandoffPostDefault handles this case with default header values.

internal server error
*/
type StorageServiceHintedHandoffPostDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the storage service hinted handoff post default response
func (o *StorageServiceHintedHandoffPostDefault) Code() int {
	return o._statusCode
}

func (o *StorageServiceHintedHandoffPostDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *StorageServiceHintedHandoffPostDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *StorageServiceHintedHandoffPostDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
