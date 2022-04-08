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

// ColumnFamilyMetricsReadLatencyByNameGetReader is a Reader for the ColumnFamilyMetricsReadLatencyByNameGet structure.
type ColumnFamilyMetricsReadLatencyByNameGetReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ColumnFamilyMetricsReadLatencyByNameGetReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewColumnFamilyMetricsReadLatencyByNameGetOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewColumnFamilyMetricsReadLatencyByNameGetDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewColumnFamilyMetricsReadLatencyByNameGetOK creates a ColumnFamilyMetricsReadLatencyByNameGetOK with default headers values
func NewColumnFamilyMetricsReadLatencyByNameGetOK() *ColumnFamilyMetricsReadLatencyByNameGetOK {
	return &ColumnFamilyMetricsReadLatencyByNameGetOK{}
}

/*ColumnFamilyMetricsReadLatencyByNameGetOK handles this case with default header values.

ColumnFamilyMetricsReadLatencyByNameGetOK column family metrics read latency by name get o k
*/
type ColumnFamilyMetricsReadLatencyByNameGetOK struct {
	Payload int32
}

func (o *ColumnFamilyMetricsReadLatencyByNameGetOK) GetPayload() int32 {
	return o.Payload
}

func (o *ColumnFamilyMetricsReadLatencyByNameGetOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewColumnFamilyMetricsReadLatencyByNameGetDefault creates a ColumnFamilyMetricsReadLatencyByNameGetDefault with default headers values
func NewColumnFamilyMetricsReadLatencyByNameGetDefault(code int) *ColumnFamilyMetricsReadLatencyByNameGetDefault {
	return &ColumnFamilyMetricsReadLatencyByNameGetDefault{
		_statusCode: code,
	}
}

/*ColumnFamilyMetricsReadLatencyByNameGetDefault handles this case with default header values.

internal server error
*/
type ColumnFamilyMetricsReadLatencyByNameGetDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the column family metrics read latency by name get default response
func (o *ColumnFamilyMetricsReadLatencyByNameGetDefault) Code() int {
	return o._statusCode
}

func (o *ColumnFamilyMetricsReadLatencyByNameGetDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *ColumnFamilyMetricsReadLatencyByNameGetDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *ColumnFamilyMetricsReadLatencyByNameGetDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
