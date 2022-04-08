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

// ColumnFamilyMetricsCasPrepareEstimatedHistogramByNameGetReader is a Reader for the ColumnFamilyMetricsCasPrepareEstimatedHistogramByNameGet structure.
type ColumnFamilyMetricsCasPrepareEstimatedHistogramByNameGetReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ColumnFamilyMetricsCasPrepareEstimatedHistogramByNameGetReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewColumnFamilyMetricsCasPrepareEstimatedHistogramByNameGetOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewColumnFamilyMetricsCasPrepareEstimatedHistogramByNameGetDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewColumnFamilyMetricsCasPrepareEstimatedHistogramByNameGetOK creates a ColumnFamilyMetricsCasPrepareEstimatedHistogramByNameGetOK with default headers values
func NewColumnFamilyMetricsCasPrepareEstimatedHistogramByNameGetOK() *ColumnFamilyMetricsCasPrepareEstimatedHistogramByNameGetOK {
	return &ColumnFamilyMetricsCasPrepareEstimatedHistogramByNameGetOK{}
}

/*ColumnFamilyMetricsCasPrepareEstimatedHistogramByNameGetOK handles this case with default header values.

ColumnFamilyMetricsCasPrepareEstimatedHistogramByNameGetOK column family metrics cas prepare estimated histogram by name get o k
*/
type ColumnFamilyMetricsCasPrepareEstimatedHistogramByNameGetOK struct {
}

func (o *ColumnFamilyMetricsCasPrepareEstimatedHistogramByNameGetOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewColumnFamilyMetricsCasPrepareEstimatedHistogramByNameGetDefault creates a ColumnFamilyMetricsCasPrepareEstimatedHistogramByNameGetDefault with default headers values
func NewColumnFamilyMetricsCasPrepareEstimatedHistogramByNameGetDefault(code int) *ColumnFamilyMetricsCasPrepareEstimatedHistogramByNameGetDefault {
	return &ColumnFamilyMetricsCasPrepareEstimatedHistogramByNameGetDefault{
		_statusCode: code,
	}
}

/*ColumnFamilyMetricsCasPrepareEstimatedHistogramByNameGetDefault handles this case with default header values.

internal server error
*/
type ColumnFamilyMetricsCasPrepareEstimatedHistogramByNameGetDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the column family metrics cas prepare estimated histogram by name get default response
func (o *ColumnFamilyMetricsCasPrepareEstimatedHistogramByNameGetDefault) Code() int {
	return o._statusCode
}

func (o *ColumnFamilyMetricsCasPrepareEstimatedHistogramByNameGetDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *ColumnFamilyMetricsCasPrepareEstimatedHistogramByNameGetDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *ColumnFamilyMetricsCasPrepareEstimatedHistogramByNameGetDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
