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

// ColumnFamilySstablesPerLevelByNameGetReader is a Reader for the ColumnFamilySstablesPerLevelByNameGet structure.
type ColumnFamilySstablesPerLevelByNameGetReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ColumnFamilySstablesPerLevelByNameGetReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewColumnFamilySstablesPerLevelByNameGetOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewColumnFamilySstablesPerLevelByNameGetDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewColumnFamilySstablesPerLevelByNameGetOK creates a ColumnFamilySstablesPerLevelByNameGetOK with default headers values
func NewColumnFamilySstablesPerLevelByNameGetOK() *ColumnFamilySstablesPerLevelByNameGetOK {
	return &ColumnFamilySstablesPerLevelByNameGetOK{}
}

/*ColumnFamilySstablesPerLevelByNameGetOK handles this case with default header values.

ColumnFamilySstablesPerLevelByNameGetOK column family sstables per level by name get o k
*/
type ColumnFamilySstablesPerLevelByNameGetOK struct {
	Payload []int32
}

func (o *ColumnFamilySstablesPerLevelByNameGetOK) GetPayload() []int32 {
	return o.Payload
}

func (o *ColumnFamilySstablesPerLevelByNameGetOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewColumnFamilySstablesPerLevelByNameGetDefault creates a ColumnFamilySstablesPerLevelByNameGetDefault with default headers values
func NewColumnFamilySstablesPerLevelByNameGetDefault(code int) *ColumnFamilySstablesPerLevelByNameGetDefault {
	return &ColumnFamilySstablesPerLevelByNameGetDefault{
		_statusCode: code,
	}
}

/*ColumnFamilySstablesPerLevelByNameGetDefault handles this case with default header values.

internal server error
*/
type ColumnFamilySstablesPerLevelByNameGetDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the column family sstables per level by name get default response
func (o *ColumnFamilySstablesPerLevelByNameGetDefault) Code() int {
	return o._statusCode
}

func (o *ColumnFamilySstablesPerLevelByNameGetDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *ColumnFamilySstablesPerLevelByNameGetDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *ColumnFamilySstablesPerLevelByNameGetDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
