// Code generated by go-swagger; DO NOT EDIT.

package models

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
)

// JobInfoParams job info params
//
// swagger:model JobInfoParams
type JobInfoParams struct {

	// ID of the job
	Jobid int64 `json:"jobid,omitempty"`

	// Duration in seconds
	Wait int64 `json:"wait,omitempty"`
}

// Validate validates this job info params
func (m *JobInfoParams) Validate(formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *JobInfoParams) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *JobInfoParams) UnmarshalBinary(b []byte) error {
	var res JobInfoParams
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
