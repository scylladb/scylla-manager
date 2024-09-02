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

// ColumnFamilyMetricsLiveScannedHistogramByNameGetReader is a Reader for the ColumnFamilyMetricsLiveScannedHistogramByNameGet structure.
type ColumnFamilyMetricsLiveScannedHistogramByNameGetReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ColumnFamilyMetricsLiveScannedHistogramByNameGetReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewColumnFamilyMetricsLiveScannedHistogramByNameGetOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewColumnFamilyMetricsLiveScannedHistogramByNameGetDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewColumnFamilyMetricsLiveScannedHistogramByNameGetOK creates a ColumnFamilyMetricsLiveScannedHistogramByNameGetOK with default headers values
func NewColumnFamilyMetricsLiveScannedHistogramByNameGetOK() *ColumnFamilyMetricsLiveScannedHistogramByNameGetOK {
	return &ColumnFamilyMetricsLiveScannedHistogramByNameGetOK{}
}

/*
ColumnFamilyMetricsLiveScannedHistogramByNameGetOK handles this case with default header values.

Success
*/
type ColumnFamilyMetricsLiveScannedHistogramByNameGetOK struct {
	Payload interface{}
}

func (o *ColumnFamilyMetricsLiveScannedHistogramByNameGetOK) GetPayload() interface{} {
	return o.Payload
}

func (o *ColumnFamilyMetricsLiveScannedHistogramByNameGetOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewColumnFamilyMetricsLiveScannedHistogramByNameGetDefault creates a ColumnFamilyMetricsLiveScannedHistogramByNameGetDefault with default headers values
func NewColumnFamilyMetricsLiveScannedHistogramByNameGetDefault(code int) *ColumnFamilyMetricsLiveScannedHistogramByNameGetDefault {
	return &ColumnFamilyMetricsLiveScannedHistogramByNameGetDefault{
		_statusCode: code,
	}
}

/*
ColumnFamilyMetricsLiveScannedHistogramByNameGetDefault handles this case with default header values.

internal server error
*/
type ColumnFamilyMetricsLiveScannedHistogramByNameGetDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the column family metrics live scanned histogram by name get default response
func (o *ColumnFamilyMetricsLiveScannedHistogramByNameGetDefault) Code() int {
	return o._statusCode
}

func (o *ColumnFamilyMetricsLiveScannedHistogramByNameGetDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *ColumnFamilyMetricsLiveScannedHistogramByNameGetDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *ColumnFamilyMetricsLiveScannedHistogramByNameGetDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
