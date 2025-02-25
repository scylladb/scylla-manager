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
	"github.com/go-openapi/swag"

	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v1/models"
)

// StorageServiceTabletsRepairPostReader is a Reader for the StorageServiceTabletsRepairPost structure.
type StorageServiceTabletsRepairPostReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *StorageServiceTabletsRepairPostReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewStorageServiceTabletsRepairPostOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewStorageServiceTabletsRepairPostDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewStorageServiceTabletsRepairPostOK creates a StorageServiceTabletsRepairPostOK with default headers values
func NewStorageServiceTabletsRepairPostOK() *StorageServiceTabletsRepairPostOK {
	return &StorageServiceTabletsRepairPostOK{}
}

/*
StorageServiceTabletsRepairPostOK handles this case with default header values.

Tablet task ID
*/
type StorageServiceTabletsRepairPostOK struct {
	Payload *StorageServiceTabletsRepairPostOKBody
}

func (o *StorageServiceTabletsRepairPostOK) GetPayload() *StorageServiceTabletsRepairPostOKBody {
	return o.Payload
}

func (o *StorageServiceTabletsRepairPostOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(StorageServiceTabletsRepairPostOKBody)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewStorageServiceTabletsRepairPostDefault creates a StorageServiceTabletsRepairPostDefault with default headers values
func NewStorageServiceTabletsRepairPostDefault(code int) *StorageServiceTabletsRepairPostDefault {
	return &StorageServiceTabletsRepairPostDefault{
		_statusCode: code,
	}
}

/*
StorageServiceTabletsRepairPostDefault handles this case with default header values.

internal server error
*/
type StorageServiceTabletsRepairPostDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the storage service tablets repair post default response
func (o *StorageServiceTabletsRepairPostDefault) Code() int {
	return o._statusCode
}

func (o *StorageServiceTabletsRepairPostDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *StorageServiceTabletsRepairPostDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *StorageServiceTabletsRepairPostDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}

/*
StorageServiceTabletsRepairPostOKBody storage service tablets repair post o k body
swagger:model StorageServiceTabletsRepairPostOKBody
*/
type StorageServiceTabletsRepairPostOKBody struct {

	// Tablet task ID
	TabletTaskID string `json:"tablet_task_id,omitempty"`
}

// Validate validates this storage service tablets repair post o k body
func (o *StorageServiceTabletsRepairPostOKBody) Validate(formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (o *StorageServiceTabletsRepairPostOKBody) MarshalBinary() ([]byte, error) {
	if o == nil {
		return nil, nil
	}
	return swag.WriteJSON(o)
}

// UnmarshalBinary interface implementation
func (o *StorageServiceTabletsRepairPostOKBody) UnmarshalBinary(b []byte) error {
	var res StorageServiceTabletsRepairPostOKBody
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*o = res
	return nil
}
