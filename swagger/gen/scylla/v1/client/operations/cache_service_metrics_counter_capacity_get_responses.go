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

// CacheServiceMetricsCounterCapacityGetReader is a Reader for the CacheServiceMetricsCounterCapacityGet structure.
type CacheServiceMetricsCounterCapacityGetReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *CacheServiceMetricsCounterCapacityGetReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewCacheServiceMetricsCounterCapacityGetOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewCacheServiceMetricsCounterCapacityGetDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewCacheServiceMetricsCounterCapacityGetOK creates a CacheServiceMetricsCounterCapacityGetOK with default headers values
func NewCacheServiceMetricsCounterCapacityGetOK() *CacheServiceMetricsCounterCapacityGetOK {
	return &CacheServiceMetricsCounterCapacityGetOK{}
}

/*CacheServiceMetricsCounterCapacityGetOK handles this case with default header values.

CacheServiceMetricsCounterCapacityGetOK cache service metrics counter capacity get o k
*/
type CacheServiceMetricsCounterCapacityGetOK struct {
	Payload interface{}
}

func (o *CacheServiceMetricsCounterCapacityGetOK) GetPayload() interface{} {
	return o.Payload
}

func (o *CacheServiceMetricsCounterCapacityGetOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewCacheServiceMetricsCounterCapacityGetDefault creates a CacheServiceMetricsCounterCapacityGetDefault with default headers values
func NewCacheServiceMetricsCounterCapacityGetDefault(code int) *CacheServiceMetricsCounterCapacityGetDefault {
	return &CacheServiceMetricsCounterCapacityGetDefault{
		_statusCode: code,
	}
}

/*CacheServiceMetricsCounterCapacityGetDefault handles this case with default header values.

internal server error
*/
type CacheServiceMetricsCounterCapacityGetDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the cache service metrics counter capacity get default response
func (o *CacheServiceMetricsCounterCapacityGetDefault) Code() int {
	return o._statusCode
}

func (o *CacheServiceMetricsCounterCapacityGetDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *CacheServiceMetricsCounterCapacityGetDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *CacheServiceMetricsCounterCapacityGetDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
