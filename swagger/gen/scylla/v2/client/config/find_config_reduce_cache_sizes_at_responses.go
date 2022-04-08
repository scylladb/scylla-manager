// Code generated by go-swagger; DO NOT EDIT.

package config

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"
	"strings"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"

	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla/v2/models"
)

// FindConfigReduceCacheSizesAtReader is a Reader for the FindConfigReduceCacheSizesAt structure.
type FindConfigReduceCacheSizesAtReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *FindConfigReduceCacheSizesAtReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewFindConfigReduceCacheSizesAtOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewFindConfigReduceCacheSizesAtDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewFindConfigReduceCacheSizesAtOK creates a FindConfigReduceCacheSizesAtOK with default headers values
func NewFindConfigReduceCacheSizesAtOK() *FindConfigReduceCacheSizesAtOK {
	return &FindConfigReduceCacheSizesAtOK{}
}

/*FindConfigReduceCacheSizesAtOK handles this case with default header values.

Config value
*/
type FindConfigReduceCacheSizesAtOK struct {
	Payload float64
}

func (o *FindConfigReduceCacheSizesAtOK) GetPayload() float64 {
	return o.Payload
}

func (o *FindConfigReduceCacheSizesAtOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewFindConfigReduceCacheSizesAtDefault creates a FindConfigReduceCacheSizesAtDefault with default headers values
func NewFindConfigReduceCacheSizesAtDefault(code int) *FindConfigReduceCacheSizesAtDefault {
	return &FindConfigReduceCacheSizesAtDefault{
		_statusCode: code,
	}
}

/*FindConfigReduceCacheSizesAtDefault handles this case with default header values.

unexpected error
*/
type FindConfigReduceCacheSizesAtDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the find config reduce cache sizes at default response
func (o *FindConfigReduceCacheSizesAtDefault) Code() int {
	return o._statusCode
}

func (o *FindConfigReduceCacheSizesAtDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *FindConfigReduceCacheSizesAtDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *FindConfigReduceCacheSizesAtDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
