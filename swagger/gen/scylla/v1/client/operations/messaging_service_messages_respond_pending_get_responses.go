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

// MessagingServiceMessagesRespondPendingGetReader is a Reader for the MessagingServiceMessagesRespondPendingGet structure.
type MessagingServiceMessagesRespondPendingGetReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *MessagingServiceMessagesRespondPendingGetReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewMessagingServiceMessagesRespondPendingGetOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewMessagingServiceMessagesRespondPendingGetDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewMessagingServiceMessagesRespondPendingGetOK creates a MessagingServiceMessagesRespondPendingGetOK with default headers values
func NewMessagingServiceMessagesRespondPendingGetOK() *MessagingServiceMessagesRespondPendingGetOK {
	return &MessagingServiceMessagesRespondPendingGetOK{}
}

/*MessagingServiceMessagesRespondPendingGetOK handles this case with default header values.

MessagingServiceMessagesRespondPendingGetOK messaging service messages respond pending get o k
*/
type MessagingServiceMessagesRespondPendingGetOK struct {
	Payload []*models.MessageCounter
}

func (o *MessagingServiceMessagesRespondPendingGetOK) GetPayload() []*models.MessageCounter {
	return o.Payload
}

func (o *MessagingServiceMessagesRespondPendingGetOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewMessagingServiceMessagesRespondPendingGetDefault creates a MessagingServiceMessagesRespondPendingGetDefault with default headers values
func NewMessagingServiceMessagesRespondPendingGetDefault(code int) *MessagingServiceMessagesRespondPendingGetDefault {
	return &MessagingServiceMessagesRespondPendingGetDefault{
		_statusCode: code,
	}
}

/*MessagingServiceMessagesRespondPendingGetDefault handles this case with default header values.

internal server error
*/
type MessagingServiceMessagesRespondPendingGetDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the messaging service messages respond pending get default response
func (o *MessagingServiceMessagesRespondPendingGetDefault) Code() int {
	return o._statusCode
}

func (o *MessagingServiceMessagesRespondPendingGetDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *MessagingServiceMessagesRespondPendingGetDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *MessagingServiceMessagesRespondPendingGetDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
