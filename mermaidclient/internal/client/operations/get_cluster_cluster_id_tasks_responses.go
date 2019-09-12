// Code generated by go-swagger; DO NOT EDIT.

package operations

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"

	"github.com/go-openapi/runtime"

	strfmt "github.com/go-openapi/strfmt"

	models "github.com/scylladb/mermaid/mermaidclient/internal/models"
)

// GetClusterClusterIDTasksReader is a Reader for the GetClusterClusterIDTasks structure.
type GetClusterClusterIDTasksReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *GetClusterClusterIDTasksReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {

	case 200:
		result := NewGetClusterClusterIDTasksOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil

	case 400:
		result := NewGetClusterClusterIDTasksBadRequest()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	case 404:
		result := NewGetClusterClusterIDTasksNotFound()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	case 500:
		result := NewGetClusterClusterIDTasksInternalServerError()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return nil, result

	default:
		result := NewGetClusterClusterIDTasksDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewGetClusterClusterIDTasksOK creates a GetClusterClusterIDTasksOK with default headers values
func NewGetClusterClusterIDTasksOK() *GetClusterClusterIDTasksOK {
	return &GetClusterClusterIDTasksOK{}
}

/*GetClusterClusterIDTasksOK handles this case with default header values.

List of tasks
*/
type GetClusterClusterIDTasksOK struct {
	Payload []*models.ExtendedTask
}

func (o *GetClusterClusterIDTasksOK) Error() string {
	return fmt.Sprintf("[GET /cluster/{cluster_id}/tasks][%d] getClusterClusterIdTasksOK  %+v", 200, o.Payload)
}

func (o *GetClusterClusterIDTasksOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewGetClusterClusterIDTasksBadRequest creates a GetClusterClusterIDTasksBadRequest with default headers values
func NewGetClusterClusterIDTasksBadRequest() *GetClusterClusterIDTasksBadRequest {
	return &GetClusterClusterIDTasksBadRequest{}
}

/*GetClusterClusterIDTasksBadRequest handles this case with default header values.

Bad Request
*/
type GetClusterClusterIDTasksBadRequest struct {
	Payload *models.ErrorResponse
}

func (o *GetClusterClusterIDTasksBadRequest) Error() string {
	return fmt.Sprintf("[GET /cluster/{cluster_id}/tasks][%d] getClusterClusterIdTasksBadRequest  %+v", 400, o.Payload)
}

func (o *GetClusterClusterIDTasksBadRequest) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewGetClusterClusterIDTasksNotFound creates a GetClusterClusterIDTasksNotFound with default headers values
func NewGetClusterClusterIDTasksNotFound() *GetClusterClusterIDTasksNotFound {
	return &GetClusterClusterIDTasksNotFound{}
}

/*GetClusterClusterIDTasksNotFound handles this case with default header values.

Not found
*/
type GetClusterClusterIDTasksNotFound struct {
	Payload *models.ErrorResponse
}

func (o *GetClusterClusterIDTasksNotFound) Error() string {
	return fmt.Sprintf("[GET /cluster/{cluster_id}/tasks][%d] getClusterClusterIdTasksNotFound  %+v", 404, o.Payload)
}

func (o *GetClusterClusterIDTasksNotFound) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewGetClusterClusterIDTasksInternalServerError creates a GetClusterClusterIDTasksInternalServerError with default headers values
func NewGetClusterClusterIDTasksInternalServerError() *GetClusterClusterIDTasksInternalServerError {
	return &GetClusterClusterIDTasksInternalServerError{}
}

/*GetClusterClusterIDTasksInternalServerError handles this case with default header values.

Server error
*/
type GetClusterClusterIDTasksInternalServerError struct {
	Payload *models.ErrorResponse
}

func (o *GetClusterClusterIDTasksInternalServerError) Error() string {
	return fmt.Sprintf("[GET /cluster/{cluster_id}/tasks][%d] getClusterClusterIdTasksInternalServerError  %+v", 500, o.Payload)
}

func (o *GetClusterClusterIDTasksInternalServerError) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewGetClusterClusterIDTasksDefault creates a GetClusterClusterIDTasksDefault with default headers values
func NewGetClusterClusterIDTasksDefault(code int) *GetClusterClusterIDTasksDefault {
	return &GetClusterClusterIDTasksDefault{
		_statusCode: code,
	}
}

/*GetClusterClusterIDTasksDefault handles this case with default header values.

Unexpected error
*/
type GetClusterClusterIDTasksDefault struct {
	_statusCode int

	Payload *models.ErrorResponse
}

// Code gets the status code for the get cluster cluster ID tasks default response
func (o *GetClusterClusterIDTasksDefault) Code() int {
	return o._statusCode
}

func (o *GetClusterClusterIDTasksDefault) Error() string {
	return fmt.Sprintf("[GET /cluster/{cluster_id}/tasks][%d] GetClusterClusterIDTasks default  %+v", o._statusCode, o.Payload)
}

func (o *GetClusterClusterIDTasksDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}
