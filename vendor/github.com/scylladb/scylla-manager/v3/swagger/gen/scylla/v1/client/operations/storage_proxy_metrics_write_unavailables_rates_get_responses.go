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

// StorageProxyMetricsWriteUnavailablesRatesGetReader is a Reader for the StorageProxyMetricsWriteUnavailablesRatesGet structure.
type StorageProxyMetricsWriteUnavailablesRatesGetReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *StorageProxyMetricsWriteUnavailablesRatesGetReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewStorageProxyMetricsWriteUnavailablesRatesGetOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewStorageProxyMetricsWriteUnavailablesRatesGetDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewStorageProxyMetricsWriteUnavailablesRatesGetOK creates a StorageProxyMetricsWriteUnavailablesRatesGetOK with default headers values
func NewStorageProxyMetricsWriteUnavailablesRatesGetOK() *StorageProxyMetricsWriteUnavailablesRatesGetOK {
	return &StorageProxyMetricsWriteUnavailablesRatesGetOK{}
}

/*
StorageProxyMetricsWriteUnavailablesRatesGetOK handles this case with default header values.

Success
*/
type StorageProxyMetricsWriteUnavailablesRatesGetOK struct {
	Payload *models.RateMovingAverage
}

func (o *StorageProxyMetricsWriteUnavailablesRatesGetOK) GetPayload() *models.RateMovingAverage {
	return o.Payload
}

func (o *StorageProxyMetricsWriteUnavailablesRatesGetOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.RateMovingAverage)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewStorageProxyMetricsWriteUnavailablesRatesGetDefault creates a StorageProxyMetricsWriteUnavailablesRatesGetDefault with default headers values
func NewStorageProxyMetricsWriteUnavailablesRatesGetDefault(code int) *StorageProxyMetricsWriteUnavailablesRatesGetDefault {
	return &StorageProxyMetricsWriteUnavailablesRatesGetDefault{
		_statusCode: code,
	}
}

/*
StorageProxyMetricsWriteUnavailablesRatesGetDefault handles this case with default header values.

internal server error
*/
type StorageProxyMetricsWriteUnavailablesRatesGetDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the storage proxy metrics write unavailables rates get default response
func (o *StorageProxyMetricsWriteUnavailablesRatesGetDefault) Code() int {
	return o._statusCode
}

func (o *StorageProxyMetricsWriteUnavailablesRatesGetDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *StorageProxyMetricsWriteUnavailablesRatesGetDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *StorageProxyMetricsWriteUnavailablesRatesGetDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
