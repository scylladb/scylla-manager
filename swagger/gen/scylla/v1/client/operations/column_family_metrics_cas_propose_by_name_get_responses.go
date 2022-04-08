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

// ColumnFamilyMetricsCasProposeByNameGetReader is a Reader for the ColumnFamilyMetricsCasProposeByNameGet structure.
type ColumnFamilyMetricsCasProposeByNameGetReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ColumnFamilyMetricsCasProposeByNameGetReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewColumnFamilyMetricsCasProposeByNameGetOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewColumnFamilyMetricsCasProposeByNameGetDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewColumnFamilyMetricsCasProposeByNameGetOK creates a ColumnFamilyMetricsCasProposeByNameGetOK with default headers values
func NewColumnFamilyMetricsCasProposeByNameGetOK() *ColumnFamilyMetricsCasProposeByNameGetOK {
	return &ColumnFamilyMetricsCasProposeByNameGetOK{}
}

/*ColumnFamilyMetricsCasProposeByNameGetOK handles this case with default header values.

ColumnFamilyMetricsCasProposeByNameGetOK column family metrics cas propose by name get o k
*/
type ColumnFamilyMetricsCasProposeByNameGetOK struct {
	Payload int32
}

func (o *ColumnFamilyMetricsCasProposeByNameGetOK) GetPayload() int32 {
	return o.Payload
}

func (o *ColumnFamilyMetricsCasProposeByNameGetOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewColumnFamilyMetricsCasProposeByNameGetDefault creates a ColumnFamilyMetricsCasProposeByNameGetDefault with default headers values
func NewColumnFamilyMetricsCasProposeByNameGetDefault(code int) *ColumnFamilyMetricsCasProposeByNameGetDefault {
	return &ColumnFamilyMetricsCasProposeByNameGetDefault{
		_statusCode: code,
	}
}

/*ColumnFamilyMetricsCasProposeByNameGetDefault handles this case with default header values.

internal server error
*/
type ColumnFamilyMetricsCasProposeByNameGetDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the column family metrics cas propose by name get default response
func (o *ColumnFamilyMetricsCasProposeByNameGetDefault) Code() int {
	return o._statusCode
}

func (o *ColumnFamilyMetricsCasProposeByNameGetDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *ColumnFamilyMetricsCasProposeByNameGetDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *ColumnFamilyMetricsCasProposeByNameGetDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
