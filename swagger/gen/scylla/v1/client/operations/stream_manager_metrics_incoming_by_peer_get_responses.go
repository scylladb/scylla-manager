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

// StreamManagerMetricsIncomingByPeerGetReader is a Reader for the StreamManagerMetricsIncomingByPeerGet structure.
type StreamManagerMetricsIncomingByPeerGetReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *StreamManagerMetricsIncomingByPeerGetReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewStreamManagerMetricsIncomingByPeerGetOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewStreamManagerMetricsIncomingByPeerGetDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewStreamManagerMetricsIncomingByPeerGetOK creates a StreamManagerMetricsIncomingByPeerGetOK with default headers values
func NewStreamManagerMetricsIncomingByPeerGetOK() *StreamManagerMetricsIncomingByPeerGetOK {
	return &StreamManagerMetricsIncomingByPeerGetOK{}
}

/*StreamManagerMetricsIncomingByPeerGetOK handles this case with default header values.

StreamManagerMetricsIncomingByPeerGetOK stream manager metrics incoming by peer get o k
*/
type StreamManagerMetricsIncomingByPeerGetOK struct {
	Payload int32
}

func (o *StreamManagerMetricsIncomingByPeerGetOK) GetPayload() int32 {
	return o.Payload
}

func (o *StreamManagerMetricsIncomingByPeerGetOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewStreamManagerMetricsIncomingByPeerGetDefault creates a StreamManagerMetricsIncomingByPeerGetDefault with default headers values
func NewStreamManagerMetricsIncomingByPeerGetDefault(code int) *StreamManagerMetricsIncomingByPeerGetDefault {
	return &StreamManagerMetricsIncomingByPeerGetDefault{
		_statusCode: code,
	}
}

/*StreamManagerMetricsIncomingByPeerGetDefault handles this case with default header values.

internal server error
*/
type StreamManagerMetricsIncomingByPeerGetDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the stream manager metrics incoming by peer get default response
func (o *StreamManagerMetricsIncomingByPeerGetDefault) Code() int {
	return o._statusCode
}

func (o *StreamManagerMetricsIncomingByPeerGetDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *StreamManagerMetricsIncomingByPeerGetDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *StreamManagerMetricsIncomingByPeerGetDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
