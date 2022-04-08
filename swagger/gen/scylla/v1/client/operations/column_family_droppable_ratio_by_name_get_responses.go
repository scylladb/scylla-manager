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

// ColumnFamilyDroppableRatioByNameGetReader is a Reader for the ColumnFamilyDroppableRatioByNameGet structure.
type ColumnFamilyDroppableRatioByNameGetReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ColumnFamilyDroppableRatioByNameGetReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewColumnFamilyDroppableRatioByNameGetOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewColumnFamilyDroppableRatioByNameGetDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewColumnFamilyDroppableRatioByNameGetOK creates a ColumnFamilyDroppableRatioByNameGetOK with default headers values
func NewColumnFamilyDroppableRatioByNameGetOK() *ColumnFamilyDroppableRatioByNameGetOK {
	return &ColumnFamilyDroppableRatioByNameGetOK{}
}

/*ColumnFamilyDroppableRatioByNameGetOK handles this case with default header values.

ColumnFamilyDroppableRatioByNameGetOK column family droppable ratio by name get o k
*/
type ColumnFamilyDroppableRatioByNameGetOK struct {
	Payload interface{}
}

func (o *ColumnFamilyDroppableRatioByNameGetOK) GetPayload() interface{} {
	return o.Payload
}

func (o *ColumnFamilyDroppableRatioByNameGetOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewColumnFamilyDroppableRatioByNameGetDefault creates a ColumnFamilyDroppableRatioByNameGetDefault with default headers values
func NewColumnFamilyDroppableRatioByNameGetDefault(code int) *ColumnFamilyDroppableRatioByNameGetDefault {
	return &ColumnFamilyDroppableRatioByNameGetDefault{
		_statusCode: code,
	}
}

/*ColumnFamilyDroppableRatioByNameGetDefault handles this case with default header values.

internal server error
*/
type ColumnFamilyDroppableRatioByNameGetDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the column family droppable ratio by name get default response
func (o *ColumnFamilyDroppableRatioByNameGetDefault) Code() int {
	return o._statusCode
}

func (o *ColumnFamilyDroppableRatioByNameGetDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *ColumnFamilyDroppableRatioByNameGetDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *ColumnFamilyDroppableRatioByNameGetDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
