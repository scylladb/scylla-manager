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

// CacheServiceCounterCacheSavePeriodPostReader is a Reader for the CacheServiceCounterCacheSavePeriodPost structure.
type CacheServiceCounterCacheSavePeriodPostReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *CacheServiceCounterCacheSavePeriodPostReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewCacheServiceCounterCacheSavePeriodPostOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewCacheServiceCounterCacheSavePeriodPostDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewCacheServiceCounterCacheSavePeriodPostOK creates a CacheServiceCounterCacheSavePeriodPostOK with default headers values
func NewCacheServiceCounterCacheSavePeriodPostOK() *CacheServiceCounterCacheSavePeriodPostOK {
	return &CacheServiceCounterCacheSavePeriodPostOK{}
}

/*CacheServiceCounterCacheSavePeriodPostOK handles this case with default header values.

CacheServiceCounterCacheSavePeriodPostOK cache service counter cache save period post o k
*/
type CacheServiceCounterCacheSavePeriodPostOK struct {
}

func (o *CacheServiceCounterCacheSavePeriodPostOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewCacheServiceCounterCacheSavePeriodPostDefault creates a CacheServiceCounterCacheSavePeriodPostDefault with default headers values
func NewCacheServiceCounterCacheSavePeriodPostDefault(code int) *CacheServiceCounterCacheSavePeriodPostDefault {
	return &CacheServiceCounterCacheSavePeriodPostDefault{
		_statusCode: code,
	}
}

/*CacheServiceCounterCacheSavePeriodPostDefault handles this case with default header values.

internal server error
*/
type CacheServiceCounterCacheSavePeriodPostDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the cache service counter cache save period post default response
func (o *CacheServiceCounterCacheSavePeriodPostDefault) Code() int {
	return o._statusCode
}

func (o *CacheServiceCounterCacheSavePeriodPostDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *CacheServiceCounterCacheSavePeriodPostDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *CacheServiceCounterCacheSavePeriodPostDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
