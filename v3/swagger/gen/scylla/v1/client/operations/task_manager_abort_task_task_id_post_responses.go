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

// TaskManagerAbortTaskTaskIDPostReader is a Reader for the TaskManagerAbortTaskTaskIDPost structure.
type TaskManagerAbortTaskTaskIDPostReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *TaskManagerAbortTaskTaskIDPostReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewTaskManagerAbortTaskTaskIDPostOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewTaskManagerAbortTaskTaskIDPostDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewTaskManagerAbortTaskTaskIDPostOK creates a TaskManagerAbortTaskTaskIDPostOK with default headers values
func NewTaskManagerAbortTaskTaskIDPostOK() *TaskManagerAbortTaskTaskIDPostOK {
	return &TaskManagerAbortTaskTaskIDPostOK{}
}

/*
TaskManagerAbortTaskTaskIDPostOK handles this case with default header values.

Success
*/
type TaskManagerAbortTaskTaskIDPostOK struct {
}

func (o *TaskManagerAbortTaskTaskIDPostOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	return nil
}

// NewTaskManagerAbortTaskTaskIDPostDefault creates a TaskManagerAbortTaskTaskIDPostDefault with default headers values
func NewTaskManagerAbortTaskTaskIDPostDefault(code int) *TaskManagerAbortTaskTaskIDPostDefault {
	return &TaskManagerAbortTaskTaskIDPostDefault{
		_statusCode: code,
	}
}

/*
TaskManagerAbortTaskTaskIDPostDefault handles this case with default header values.

internal server error
*/
type TaskManagerAbortTaskTaskIDPostDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the task manager abort task task Id post default response
func (o *TaskManagerAbortTaskTaskIDPostDefault) Code() int {
	return o._statusCode
}

func (o *TaskManagerAbortTaskTaskIDPostDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *TaskManagerAbortTaskTaskIDPostDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *TaskManagerAbortTaskTaskIDPostDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
