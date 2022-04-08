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

// CacheServiceMetricsRowHitsMovingAvrageGetReader is a Reader for the CacheServiceMetricsRowHitsMovingAvrageGet structure.
type CacheServiceMetricsRowHitsMovingAvrageGetReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *CacheServiceMetricsRowHitsMovingAvrageGetReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewCacheServiceMetricsRowHitsMovingAvrageGetOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewCacheServiceMetricsRowHitsMovingAvrageGetDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewCacheServiceMetricsRowHitsMovingAvrageGetOK creates a CacheServiceMetricsRowHitsMovingAvrageGetOK with default headers values
func NewCacheServiceMetricsRowHitsMovingAvrageGetOK() *CacheServiceMetricsRowHitsMovingAvrageGetOK {
	return &CacheServiceMetricsRowHitsMovingAvrageGetOK{}
}

/*CacheServiceMetricsRowHitsMovingAvrageGetOK handles this case with default header values.

CacheServiceMetricsRowHitsMovingAvrageGetOK cache service metrics row hits moving avrage get o k
*/
type CacheServiceMetricsRowHitsMovingAvrageGetOK struct {
	Payload *models.RateMovingAverage
}

func (o *CacheServiceMetricsRowHitsMovingAvrageGetOK) GetPayload() *models.RateMovingAverage {
	return o.Payload
}

func (o *CacheServiceMetricsRowHitsMovingAvrageGetOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.RateMovingAverage)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewCacheServiceMetricsRowHitsMovingAvrageGetDefault creates a CacheServiceMetricsRowHitsMovingAvrageGetDefault with default headers values
func NewCacheServiceMetricsRowHitsMovingAvrageGetDefault(code int) *CacheServiceMetricsRowHitsMovingAvrageGetDefault {
	return &CacheServiceMetricsRowHitsMovingAvrageGetDefault{
		_statusCode: code,
	}
}

/*CacheServiceMetricsRowHitsMovingAvrageGetDefault handles this case with default header values.

internal server error
*/
type CacheServiceMetricsRowHitsMovingAvrageGetDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the cache service metrics row hits moving avrage get default response
func (o *CacheServiceMetricsRowHitsMovingAvrageGetDefault) Code() int {
	return o._statusCode
}

func (o *CacheServiceMetricsRowHitsMovingAvrageGetDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *CacheServiceMetricsRowHitsMovingAvrageGetDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *CacheServiceMetricsRowHitsMovingAvrageGetDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
