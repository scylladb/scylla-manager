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

// CacheServiceMetricsCounterRequestsMovingAvrageGetReader is a Reader for the CacheServiceMetricsCounterRequestsMovingAvrageGet structure.
type CacheServiceMetricsCounterRequestsMovingAvrageGetReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *CacheServiceMetricsCounterRequestsMovingAvrageGetReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewCacheServiceMetricsCounterRequestsMovingAvrageGetOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewCacheServiceMetricsCounterRequestsMovingAvrageGetDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewCacheServiceMetricsCounterRequestsMovingAvrageGetOK creates a CacheServiceMetricsCounterRequestsMovingAvrageGetOK with default headers values
func NewCacheServiceMetricsCounterRequestsMovingAvrageGetOK() *CacheServiceMetricsCounterRequestsMovingAvrageGetOK {
	return &CacheServiceMetricsCounterRequestsMovingAvrageGetOK{}
}

/*
CacheServiceMetricsCounterRequestsMovingAvrageGetOK handles this case with default header values.

Success
*/
type CacheServiceMetricsCounterRequestsMovingAvrageGetOK struct {
	Payload *models.RateMovingAverage
}

func (o *CacheServiceMetricsCounterRequestsMovingAvrageGetOK) GetPayload() *models.RateMovingAverage {
	return o.Payload
}

func (o *CacheServiceMetricsCounterRequestsMovingAvrageGetOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.RateMovingAverage)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewCacheServiceMetricsCounterRequestsMovingAvrageGetDefault creates a CacheServiceMetricsCounterRequestsMovingAvrageGetDefault with default headers values
func NewCacheServiceMetricsCounterRequestsMovingAvrageGetDefault(code int) *CacheServiceMetricsCounterRequestsMovingAvrageGetDefault {
	return &CacheServiceMetricsCounterRequestsMovingAvrageGetDefault{
		_statusCode: code,
	}
}

/*
CacheServiceMetricsCounterRequestsMovingAvrageGetDefault handles this case with default header values.

internal server error
*/
type CacheServiceMetricsCounterRequestsMovingAvrageGetDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the cache service metrics counter requests moving avrage get default response
func (o *CacheServiceMetricsCounterRequestsMovingAvrageGetDefault) Code() int {
	return o._statusCode
}

func (o *CacheServiceMetricsCounterRequestsMovingAvrageGetDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *CacheServiceMetricsCounterRequestsMovingAvrageGetDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *CacheServiceMetricsCounterRequestsMovingAvrageGetDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
