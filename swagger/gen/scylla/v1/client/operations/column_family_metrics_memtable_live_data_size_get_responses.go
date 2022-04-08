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

// ColumnFamilyMetricsMemtableLiveDataSizeGetReader is a Reader for the ColumnFamilyMetricsMemtableLiveDataSizeGet structure.
type ColumnFamilyMetricsMemtableLiveDataSizeGetReader struct {
	formats strfmt.Registry
}

// ReadResponse reads a server response into the received o.
func (o *ColumnFamilyMetricsMemtableLiveDataSizeGetReader) ReadResponse(response runtime.ClientResponse, consumer runtime.Consumer) (interface{}, error) {
	switch response.Code() {
	case 200:
		result := NewColumnFamilyMetricsMemtableLiveDataSizeGetOK()
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		return result, nil
	default:
		result := NewColumnFamilyMetricsMemtableLiveDataSizeGetDefault(response.Code())
		if err := result.readResponse(response, consumer, o.formats); err != nil {
			return nil, err
		}
		if response.Code()/100 == 2 {
			return result, nil
		}
		return nil, result
	}
}

// NewColumnFamilyMetricsMemtableLiveDataSizeGetOK creates a ColumnFamilyMetricsMemtableLiveDataSizeGetOK with default headers values
func NewColumnFamilyMetricsMemtableLiveDataSizeGetOK() *ColumnFamilyMetricsMemtableLiveDataSizeGetOK {
	return &ColumnFamilyMetricsMemtableLiveDataSizeGetOK{}
}

/*ColumnFamilyMetricsMemtableLiveDataSizeGetOK handles this case with default header values.

ColumnFamilyMetricsMemtableLiveDataSizeGetOK column family metrics memtable live data size get o k
*/
type ColumnFamilyMetricsMemtableLiveDataSizeGetOK struct {
	Payload interface{}
}

func (o *ColumnFamilyMetricsMemtableLiveDataSizeGetOK) GetPayload() interface{} {
	return o.Payload
}

func (o *ColumnFamilyMetricsMemtableLiveDataSizeGetOK) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	// response payload
	if err := consumer.Consume(response.Body(), &o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

// NewColumnFamilyMetricsMemtableLiveDataSizeGetDefault creates a ColumnFamilyMetricsMemtableLiveDataSizeGetDefault with default headers values
func NewColumnFamilyMetricsMemtableLiveDataSizeGetDefault(code int) *ColumnFamilyMetricsMemtableLiveDataSizeGetDefault {
	return &ColumnFamilyMetricsMemtableLiveDataSizeGetDefault{
		_statusCode: code,
	}
}

/*ColumnFamilyMetricsMemtableLiveDataSizeGetDefault handles this case with default header values.

internal server error
*/
type ColumnFamilyMetricsMemtableLiveDataSizeGetDefault struct {
	_statusCode int

	Payload *models.ErrorModel
}

// Code gets the status code for the column family metrics memtable live data size get default response
func (o *ColumnFamilyMetricsMemtableLiveDataSizeGetDefault) Code() int {
	return o._statusCode
}

func (o *ColumnFamilyMetricsMemtableLiveDataSizeGetDefault) GetPayload() *models.ErrorModel {
	return o.Payload
}

func (o *ColumnFamilyMetricsMemtableLiveDataSizeGetDefault) readResponse(response runtime.ClientResponse, consumer runtime.Consumer, formats strfmt.Registry) error {

	o.Payload = new(models.ErrorModel)

	// response payload
	if err := consumer.Consume(response.Body(), o.Payload); err != nil && err != io.EOF {
		return err
	}

	return nil
}

func (o *ColumnFamilyMetricsMemtableLiveDataSizeGetDefault) Error() string {
	return fmt.Sprintf("agent [HTTP %d] %s", o._statusCode, strings.TrimRight(o.Payload.Message, "."))
}
