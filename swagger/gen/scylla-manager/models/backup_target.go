// Code generated by go-swagger; DO NOT EDIT.

package models

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"strconv"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
)

// BackupTarget backup target
//
// swagger:model BackupTarget
type BackupTarget struct {

	// cluster id
	ClusterID string `json:"cluster_id,omitempty"`

	// dc
	Dc []string `json:"dc"`

	// host
	Host string `json:"host,omitempty"`

	// location
	Location []string `json:"location"`

	// rate limit
	RateLimit []string `json:"rate_limit"`

	// retention policy
	RetentionPolicy *RetentionPolicy `json:"retention_policy,omitempty"`

	// size
	Size int64 `json:"size,omitempty"`

	// snapshot parallel
	SnapshotParallel []string `json:"snapshot_parallel"`

	// units
	Units []*BackupUnit `json:"units"`

	// upload parallel
	UploadParallel []string `json:"upload_parallel"`

	// with hosts
	WithHosts []string `json:"with_hosts"`
}

// Validate validates this backup target
func (m *BackupTarget) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateRetentionPolicy(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateUnits(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *BackupTarget) validateRetentionPolicy(formats strfmt.Registry) error {

	if swag.IsZero(m.RetentionPolicy) { // not required
		return nil
	}

	if m.RetentionPolicy != nil {
		if err := m.RetentionPolicy.Validate(formats); err != nil {
			if ve, ok := err.(*errors.Validation); ok {
				return ve.ValidateName("retention_policy")
			}
			return err
		}
	}

	return nil
}

func (m *BackupTarget) validateUnits(formats strfmt.Registry) error {

	if swag.IsZero(m.Units) { // not required
		return nil
	}

	for i := 0; i < len(m.Units); i++ {
		if swag.IsZero(m.Units[i]) { // not required
			continue
		}

		if m.Units[i] != nil {
			if err := m.Units[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("units" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

// MarshalBinary interface implementation
func (m *BackupTarget) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *BackupTarget) UnmarshalBinary(b []byte) error {
	var res BackupTarget
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
