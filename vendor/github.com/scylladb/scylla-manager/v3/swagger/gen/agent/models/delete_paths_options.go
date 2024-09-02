// Code generated by go-swagger; DO NOT EDIT.

package models

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
)

// DeletePathsOptions delete paths options
//
// swagger:model DeletePathsOptions
type DeletePathsOptions struct {

	// File system e.g. s3: or gcs:
	Fs string `json:"fs,omitempty"`

	// Paths relative to remote eg. file.txt
	Paths []string `json:"paths"`

	// A directory within that remote eg. files/
	Remote string `json:"remote,omitempty"`
}

// Validate validates this delete paths options
func (m *DeletePathsOptions) Validate(formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *DeletePathsOptions) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *DeletePathsOptions) UnmarshalBinary(b []byte) error {
	var res DeletePathsOptions
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
