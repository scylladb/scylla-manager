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

// ColumnFamilyMetricsIndexSummaryOffHeapMemoryUsedGetReader is a Reader for the ColumnFamilyMetricsIndexSummaryOffHeapMemoryUsedGet structure.
type ColumnFamilyMetricsIndexSummaryOffHeapMemoryUsedGetReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ColumnFamilyMetricsIndexSummaryOffHeapMemoryUsedGetReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewColumnFamilyMetricsIndexSummaryOffHeapMemoryUsedGetOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewColumnFamilyMetricsIndexSummaryOffHeapMemoryUsedGetDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewColumnFamilyMetricsIndexSummaryOffHeapMemoryUsedGetOK creates a ColumnFamilyMetricsIndexSummaryOffHeapMemoryUsedGetOK with default headers values
func NewColumnFamilyMetricsIndexSummaryOffHeapMemoryUsedGetOK() *ColumnFamilyMetricsIndexSummaryOffHeapMemoryUsedGetOK {
	return &ColumnFamilyMetricsIndexSummaryOffHeapMemoryUsedGetOK{}
}

/*ColumnFamilyMetricsIndexSummaryOffHeapMemoryUsedGetOK handles this case with default header values.

ColumnFamilyMetricsIndexSummaryOffHeapMemoryUsedGetOK column family metrics index summary off heap memory used get o k
*/
type ColumnFamilyMetricsIndexSummaryOffHeapMemoryUsedGetOK struct {
	Payload interface{}
}

func (o *ColumnFamilyMetricsIndexSummaryOffHeapMemoryUsedGetOK) GetPayload() interface{} {
	return o.Payload
}

func (o *ColumnFamilyMetricsIndexSummaryOffHeapMemoryUsedGetOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewColumnFamilyMetricsIndexSummaryOffHeapMemoryUsedGetDefault creates a ColumnFamilyMetricsIndexSummaryOffHeapMemoryUsedGetDefault with default headers values
func NewColumnFamilyMetricsIndexSummaryOffHeapMemoryUsedGetDefault(code int) *ColumnFamilyMetricsIndexSummaryOffHeapMemoryUsedGetDefault {
	return &ColumnFamilyMetricsIndexSummaryOffHeapMemoryUsedGetDefault{
		_statusCode: code,
	}
}

/*ColumnFamilyMetricsIndexSummaryOffHeapMemoryUsedGetDefault handles this case with default header values.

internal server error
*/
type ColumnFamilyMetricsIndexSummaryOffHeapMemoryUsedGetDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the column family metrics index summary off heap memory used get default response
func (o *ColumnFamilyMetricsIndexSummaryOffHeapMemoryUsedGetDefault) Code() int {
	return o._statusCode
}

func (o *ColumnFamilyMetricsIndexSummaryOffHeapMemoryUsedGetDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *ColumnFamilyMetricsIndexSummaryOffHeapMemoryUsedGetDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *ColumnFamilyMetricsIndexSummaryOffHeapMemoryUsedGetDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
