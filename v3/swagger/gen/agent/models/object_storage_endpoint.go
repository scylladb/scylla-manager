// Code generated by go-swagger; DO NOT EDIT.

package models

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
)

// ObjectStorageEndpoint Object storage endpoint configuration as described in https://github.com/scylladb/scylladb/blob/master/docs/dev/object_storage.md
//
// swagger:model ObjectStorageEndpoint
type ObjectStorageEndpoint struct {

	// aws region
	AwsRegion string `json:"aws_region,omitempty"`

	// iam role arn
	IamRoleArn string `json:"iam_role_arn,omitempty"`

	// name
	Name string `json:"name,omitempty"`

	// port
	Port int64 `json:"port,omitempty"`

	// use https
	UseHTTPS bool `json:"use_https,omitempty"`
}

// Validate validates this object storage endpoint
func (m *ObjectStorageEndpoint) Validate(formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *ObjectStorageEndpoint) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *ObjectStorageEndpoint) UnmarshalBinary(b []byte) error {
	var res ObjectStorageEndpoint
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
