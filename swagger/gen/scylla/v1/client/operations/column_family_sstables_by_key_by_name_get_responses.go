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

// ColumnFamilySstablesByKeyByNameGetReader is a Reader for the ColumnFamilySstablesByKeyByNameGet structure.
type ColumnFamilySstablesByKeyByNameGetReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ColumnFamilySstablesByKeyByNameGetReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewColumnFamilySstablesByKeyByNameGetOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewColumnFamilySstablesByKeyByNameGetDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewColumnFamilySstablesByKeyByNameGetOK creates a ColumnFamilySstablesByKeyByNameGetOK with default headers values
func NewColumnFamilySstablesByKeyByNameGetOK() *ColumnFamilySstablesByKeyByNameGetOK {
	return &ColumnFamilySstablesByKeyByNameGetOK{}
}

/*ColumnFamilySstablesByKeyByNameGetOK handles this case with default header values.

ColumnFamilySstablesByKeyByNameGetOK column family sstables by key by name get o k
*/
type ColumnFamilySstablesByKeyByNameGetOK struct {
	Payload []string
}

func (o *ColumnFamilySstablesByKeyByNameGetOK) GetPayload() []string {
	return o.Payload
}

func (o *ColumnFamilySstablesByKeyByNameGetOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewColumnFamilySstablesByKeyByNameGetDefault creates a ColumnFamilySstablesByKeyByNameGetDefault with default headers values
func NewColumnFamilySstablesByKeyByNameGetDefault(code int) *ColumnFamilySstablesByKeyByNameGetDefault {
	return &ColumnFamilySstablesByKeyByNameGetDefault{
		_statusCode: code,
	}
}

/*ColumnFamilySstablesByKeyByNameGetDefault handles this case with default header values.

internal server error
*/
type ColumnFamilySstablesByKeyByNameGetDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the column family sstables by key by name get default response
func (o *ColumnFamilySstablesByKeyByNameGetDefault) Code() int {
	return o._statusCode
}

func (o *ColumnFamilySstablesByKeyByNameGetDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *ColumnFamilySstablesByKeyByNameGetDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *ColumnFamilySstablesByKeyByNameGetDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
