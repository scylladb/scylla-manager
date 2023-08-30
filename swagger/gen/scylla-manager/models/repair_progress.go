// Code generated by go-swagger; DO NOT EDIT.

package models

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"strconv"

	"github.com/go-openapi/errors"
	"github.com/go-openapi/strfmt"
	"github.com/go-openapi/swag"
	"github.com/go-openapi/validate"
)

// RepairProgress repair progress
//
// swagger:model RepairProgress
type RepairProgress struct {

	// completed at
	// Format: date-time
	CompletedAt *strfmt.DateTime `json:"completed_at,omitempty"`

	// dcs
	Dcs []string `json:"dcs"`

	// duration ms
	DurationMs int64 `json:"duration_ms,omitempty"`

	// error
	Error int64 `json:"error,omitempty"`

	// host
	Host string `json:"host,omitempty"`

	// hosts
	Hosts []*RepairProgressHostsItems0 `json:"hosts"`

	// intensity
	Intensity float64 `json:"intensity,omitempty"`

	// max intensity
	MaxIntensity float64 `json:"max_intensity,omitempty"`

	// max parallel
	MaxParallel int64 `json:"max_parallel,omitempty"`

	// parallel
	Parallel int64 `json:"parallel,omitempty"`

	// started at
	// Format: date-time
	StartedAt *strfmt.DateTime `json:"started_at,omitempty"`

	// success
	Success int64 `json:"success,omitempty"`

	// tables
	Tables []*TableRepairProgress `json:"tables"`

	// token ranges
	TokenRanges int64 `json:"token_ranges,omitempty"`
}

// Validate validates this repair progress
func (m *RepairProgress) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateCompletedAt(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateHosts(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateStartedAt(formats); err != nil {
		res = append(res, err)
	}

	if err := m.validateTables(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *RepairProgress) validateCompletedAt(formats strfmt.Registry) error {

	if swag.IsZero(m.CompletedAt) { // not required
		return nil
	}

	if err := validate.FormatOf("completed_at", "body", "date-time", m.CompletedAt.String(), formats); err != nil {
		return err
	}

	return nil
}

func (m *RepairProgress) validateHosts(formats strfmt.Registry) error {

	if swag.IsZero(m.Hosts) { // not required
		return nil
	}

	for i := 0; i < len(m.Hosts); i++ {
		if swag.IsZero(m.Hosts[i]) { // not required
			continue
		}

		if m.Hosts[i] != nil {
			if err := m.Hosts[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("hosts" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

func (m *RepairProgress) validateStartedAt(formats strfmt.Registry) error {

	if swag.IsZero(m.StartedAt) { // not required
		return nil
	}

	if err := validate.FormatOf("started_at", "body", "date-time", m.StartedAt.String(), formats); err != nil {
		return err
	}

	return nil
}

func (m *RepairProgress) validateTables(formats strfmt.Registry) error {

	if swag.IsZero(m.Tables) { // not required
		return nil
	}

	for i := 0; i < len(m.Tables); i++ {
		if swag.IsZero(m.Tables[i]) { // not required
			continue
		}

		if m.Tables[i] != nil {
			if err := m.Tables[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("tables" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

// MarshalBinary interface implementation
func (m *RepairProgress) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *RepairProgress) UnmarshalBinary(b []byte) error {
	var res RepairProgress
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}

// RepairProgressHostsItems0 repair progress hosts items0
//
// swagger:model RepairProgressHostsItems0
type RepairProgressHostsItems0 struct {

	// host
	Host string `json:"host,omitempty"`

	// tables
	Tables []*TableRepairProgress `json:"tables"`
}

// Validate validates this repair progress hosts items0
func (m *RepairProgressHostsItems0) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateTables(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *RepairProgressHostsItems0) validateTables(formats strfmt.Registry) error {

	if swag.IsZero(m.Tables) { // not required
		return nil
	}

	for i := 0; i < len(m.Tables); i++ {
		if swag.IsZero(m.Tables[i]) { // not required
			continue
		}

		if m.Tables[i] != nil {
			if err := m.Tables[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("tables" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

// MarshalBinary interface implementation
func (m *RepairProgressHostsItems0) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *RepairProgressHostsItems0) UnmarshalBinary(b []byte) error {
	var res RepairProgressHostsItems0
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
