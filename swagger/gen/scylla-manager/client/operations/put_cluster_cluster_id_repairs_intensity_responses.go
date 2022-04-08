// Code generated by go-swagger; DO NOT EDIT.

package operations

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"

	"github.com/scylladb/scylla-manager/v3/swagger/gen/scylla-manager/models"
)

// PutClusterClusterIDRepairsIntensityReader is a Reader for the PutClusterClusterIDRepairsIntensity structure.
type PutClusterClusterIDRepairsIntensityReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *PutClusterClusterIDRepairsIntensityReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewPutClusterClusterIDRepairsIntensityOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewPutClusterClusterIDRepairsIntensityDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewPutClusterClusterIDRepairsIntensityOK creates a PutClusterClusterIDRepairsIntensityOK with default headers values
func NewPutClusterClusterIDRepairsIntensityOK() *PutClusterClusterIDRepairsIntensityOK {
	return &PutClusterClusterIDRepairsIntensityOK{}
}

/*PutClusterClusterIDRepairsIntensityOK handles this case with default header values.

OK
*/
type PutClusterClusterIDRepairsIntensityOK struct {
}

func (o *PutClusterClusterIDRepairsIntensityOK) Error() string {
	return fmt.Sprintf("[PUT /cluster/{cluster_id}/repairs/intensity][%d] putClusterClusterIdRepairsIntensityOK ", 200)
}

func (o *PutClusterClusterIDRepairsIntensityOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewPutClusterClusterIDRepairsIntensityDefault creates a PutClusterClusterIDRepairsIntensityDefault with default headers values
func NewPutClusterClusterIDRepairsIntensityDefault(code int) *PutClusterClusterIDRepairsIntensityDefault {
	return &PutClusterClusterIDRepairsIntensityDefault{
		_statusCode: code,
	}
}

/*PutClusterClusterIDRepairsIntensityDefault handles this case with default header values.

Error
*/
type PutClusterClusterIDRepairsIntensityDefault struct {
	_statusCode int

	Payload *models.ErrorResponse
}

// Code gets the status code for the put cluster cluster ID repairs intensity default response
func (o *PutClusterClusterIDRepairsIntensityDefault) Code() int {
	return o._statusCode
}

func (o *PutClusterClusterIDRepairsIntensityDefault) Error() string {
	return fmt.Sprintf("[PUT /cluster/{cluster_id}/repairs/intensity][%d] PutClusterClusterIDRepairsIntensity default  %+v", o._statusCode, o.Payload)
}

func (o *PutClusterClusterIDRepairsIntensityDefault) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *PutClusterClusterIDRepairsIntensityDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}
