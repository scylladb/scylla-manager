// Code generated by go-swagger; DO NOT EDIT.

package operations

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/go-openapi/runtime"
	"github.com/go-openapi/strfmt"

	"github.com/scylladb/scylla-manager/v3/swagger/gen/agent/models"
)

// ReadMemStatsReader is a Reader for the ReadMemStats structure.
type ReadMemStatsReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ReadMemStatsReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewReadMemStatsOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewReadMemStatsDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewReadMemStatsOK creates a ReadMemStatsOK with default headers values
func NewReadMemStatsOK() *ReadMemStatsOK {
	return &ReadMemStatsOK{}
}

/*
ReadMemStatsOK handles this case with default header values.

MemStats
*/
type ReadMemStatsOK struct {
	Payload *models.MemStats
	JobID   int64
}

func (o *ReadMemStatsOK) GetPayload() *models.MemStats {
	return o.Payload
}

func (o *ReadMemStatsOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.MemStats)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	if jobIDHeader := response.GetHeader("x-rclone-jobid"); jobIDHeader != "" {
		jobID, err := strconv.ParseInt(jobIDHeader, 10, 64)
		if err != nil {
			return err
		}

		o.JobID = jobID
	}
	return nil
}

// NewReadMemStatsDefault creates a ReadMemStatsDefault with default headers values
func NewReadMemStatsDefault(code int) *ReadMemStatsDefault {
	return &ReadMemStatsDefault{
		_statusCode: code,
	}
}

/*
ReadMemStatsDefault handles this case with default header values.

Server error
*/
type ReadMemStatsDefault struct {
	_statusCode int

	Payload *models.ErrorResponse
	JobID   int64
}

// Code gets the status code for the read mem stats default response
func (o *ReadMemStatsDefault) Code() int {
	return o._statusCode
}

func (o *ReadMemStatsDefault) GetPayload() *models.ErrorResponse {
	return o.Payload
}

func (o *ReadMemStatsDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorResponse)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	if jobIDHeader := response.GetHeader("x-rclone-jobid"); jobIDHeader != "" {
		jobID, err := strconv.ParseInt(jobIDHeader, 10, 64)
		if err != nil {
			return err
		}

		o.JobID = jobID
	}
	return nil
}

func (o *ReadMemStatsDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
