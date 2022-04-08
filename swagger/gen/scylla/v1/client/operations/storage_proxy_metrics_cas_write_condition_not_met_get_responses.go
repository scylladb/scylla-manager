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

// StorageProxyMetricsCasWriteConditionNotMetGetReader is a Reader for the StorageProxyMetricsCasWriteConditionNotMetGet structure.
type StorageProxyMetricsCasWriteConditionNotMetGetReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *StorageProxyMetricsCasWriteConditionNotMetGetReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewStorageProxyMetricsCasWriteConditionNotMetGetOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewStorageProxyMetricsCasWriteConditionNotMetGetDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewStorageProxyMetricsCasWriteConditionNotMetGetOK creates a StorageProxyMetricsCasWriteConditionNotMetGetOK with default headers values
func NewStorageProxyMetricsCasWriteConditionNotMetGetOK() *StorageProxyMetricsCasWriteConditionNotMetGetOK {
	return &StorageProxyMetricsCasWriteConditionNotMetGetOK{}
}

/*StorageProxyMetricsCasWriteConditionNotMetGetOK handles this case with default header values.

StorageProxyMetricsCasWriteConditionNotMetGetOK storage proxy metrics cas write condition not met get o k
*/
type StorageProxyMetricsCasWriteConditionNotMetGetOK struct {
	Payload int32
}

func (o *StorageProxyMetricsCasWriteConditionNotMetGetOK) GetPayload() int32 {
	return o.Payload
}

func (o *StorageProxyMetricsCasWriteConditionNotMetGetOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewStorageProxyMetricsCasWriteConditionNotMetGetDefault creates a StorageProxyMetricsCasWriteConditionNotMetGetDefault with default headers values
func NewStorageProxyMetricsCasWriteConditionNotMetGetDefault(code int) *StorageProxyMetricsCasWriteConditionNotMetGetDefault {
	return &StorageProxyMetricsCasWriteConditionNotMetGetDefault{
		_statusCode: code,
	}
}

/*StorageProxyMetricsCasWriteConditionNotMetGetDefault handles this case with default header values.

internal server error
*/
type StorageProxyMetricsCasWriteConditionNotMetGetDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the storage proxy metrics cas write condition not met get default response
func (o *StorageProxyMetricsCasWriteConditionNotMetGetDefault) Code() int {
	return o._statusCode
}

func (o *StorageProxyMetricsCasWriteConditionNotMetGetDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *StorageProxyMetricsCasWriteConditionNotMetGetDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *StorageProxyMetricsCasWriteConditionNotMetGetDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
