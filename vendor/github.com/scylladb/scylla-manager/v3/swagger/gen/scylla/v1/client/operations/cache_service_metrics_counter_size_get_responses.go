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

// CacheServiceMetricsCounterSizeGetReader is a Reader for the CacheServiceMetricsCounterSizeGet structure.
type CacheServiceMetricsCounterSizeGetReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *CacheServiceMetricsCounterSizeGetReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewCacheServiceMetricsCounterSizeGetOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewCacheServiceMetricsCounterSizeGetDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewCacheServiceMetricsCounterSizeGetOK creates a CacheServiceMetricsCounterSizeGetOK with default headers values
func NewCacheServiceMetricsCounterSizeGetOK() *CacheServiceMetricsCounterSizeGetOK {
	return &CacheServiceMetricsCounterSizeGetOK{}
}

/*
CacheServiceMetricsCounterSizeGetOK handles this case with default header values.

Success
*/
type CacheServiceMetricsCounterSizeGetOK struct {
	Payload interface{}
}

func (o *CacheServiceMetricsCounterSizeGetOK) GetPayload() interface{} {
	return o.Payload
}

func (o *CacheServiceMetricsCounterSizeGetOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewCacheServiceMetricsCounterSizeGetDefault creates a CacheServiceMetricsCounterSizeGetDefault with default headers values
func NewCacheServiceMetricsCounterSizeGetDefault(code int) *CacheServiceMetricsCounterSizeGetDefault {
	return &CacheServiceMetricsCounterSizeGetDefault{
		_statusCode: code,
	}
}

/*
CacheServiceMetricsCounterSizeGetDefault handles this case with default header values.

internal server error
*/
type CacheServiceMetricsCounterSizeGetDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the cache service metrics counter size get default response
func (o *CacheServiceMetricsCounterSizeGetDefault) Code() int {
	return o._statusCode
}

func (o *CacheServiceMetricsCounterSizeGetDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *CacheServiceMetricsCounterSizeGetDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *CacheServiceMetricsCounterSizeGetDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
