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

// TaskManagerWaitTaskTaskIDGetReader is a Reader for the TaskManagerWaitTaskTaskIDGet structure.
type TaskManagerWaitTaskTaskIDGetReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *TaskManagerWaitTaskTaskIDGetReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewTaskManagerWaitTaskTaskIDGetOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewTaskManagerWaitTaskTaskIDGetDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewTaskManagerWaitTaskTaskIDGetOK creates a TaskManagerWaitTaskTaskIDGetOK with default headers values
func NewTaskManagerWaitTaskTaskIDGetOK() *TaskManagerWaitTaskTaskIDGetOK {
	return &TaskManagerWaitTaskTaskIDGetOK{}
}

/*
TaskManagerWaitTaskTaskIDGetOK handles this case with default header values.

Success
*/
type TaskManagerWaitTaskTaskIDGetOK struct {
	Payload *models.TaskStatus
}

func (o *TaskManagerWaitTaskTaskIDGetOK) GetPayload() *models.TaskStatus {
	return o.Payload
}

func (o *TaskManagerWaitTaskTaskIDGetOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.TaskStatus)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewTaskManagerWaitTaskTaskIDGetDefault creates a TaskManagerWaitTaskTaskIDGetDefault with default headers values
func NewTaskManagerWaitTaskTaskIDGetDefault(code int) *TaskManagerWaitTaskTaskIDGetDefault {
	return &TaskManagerWaitTaskTaskIDGetDefault{
		_statusCode: code,
	}
}

/*
TaskManagerWaitTaskTaskIDGetDefault handles this case with default header values.

internal server error
*/
type TaskManagerWaitTaskTaskIDGetDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the task manager wait task task Id get default response
func (o *TaskManagerWaitTaskTaskIDGetDefault) Code() int {
	return o._statusCode
}

func (o *TaskManagerWaitTaskTaskIDGetDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *TaskManagerWaitTaskTaskIDGetDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *TaskManagerWaitTaskTaskIDGetDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}