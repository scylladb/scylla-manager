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

// FailureDetectorPhiPostReader is a Reader for the FailureDetectorPhiPost structure.
type FailureDetectorPhiPostReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *FailureDetectorPhiPostReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewFailureDetectorPhiPostOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewFailureDetectorPhiPostDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewFailureDetectorPhiPostOK creates a FailureDetectorPhiPostOK with default headers values
func NewFailureDetectorPhiPostOK() *FailureDetectorPhiPostOK {
	return &FailureDetectorPhiPostOK{}
}

/*FailureDetectorPhiPostOK handles this case with default header values.

FailureDetectorPhiPostOK failure detector phi post o k
*/
type FailureDetectorPhiPostOK struct {
	Payload interface{}
}

func (o *FailureDetectorPhiPostOK) GetPayload() interface{} {
	return o.Payload
}

func (o *FailureDetectorPhiPostOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewFailureDetectorPhiPostDefault creates a FailureDetectorPhiPostDefault with default headers values
func NewFailureDetectorPhiPostDefault(code int) *FailureDetectorPhiPostDefault {
	return &FailureDetectorPhiPostDefault{
		_statusCode: code,
	}
}

/*FailureDetectorPhiPostDefault handles this case with default header values.

internal server error
*/
type FailureDetectorPhiPostDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the failure detector phi post default response
func (o *FailureDetectorPhiPostDefault) Code() int {
	return o._statusCode
}

func (o *FailureDetectorPhiPostDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *FailureDetectorPhiPostDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *FailureDetectorPhiPostDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
