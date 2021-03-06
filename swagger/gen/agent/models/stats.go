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

// Stats stats
//
// swagger:model Stats
type Stats struct {

	// total transferred bytes since the start of the process
	Bytes int64 `json:"bytes,omitempty"`

	// an array of names of currently active file checks
	Checking []string `json:"checking"`

	// number of checked files
	Checks int64 `json:"checks,omitempty"`

	// number of deleted files
	Deletes int64 `json:"deletes,omitempty"`

	// time in seconds since the start of the process
	ElapsedTime float64 `json:"elapsedTime,omitempty"`

	// number of errors
	Errors int64 `json:"errors,omitempty"`

	// whether there has been at least one FatalError
	FatalError bool `json:"fatalError,omitempty"`

	// last occurred error
	LastError string `json:"lastError,omitempty"`

	// whether there has been at least one non-NoRetryError
	RetryError bool `json:"retryError,omitempty"`

	// average speed in bytes/sec since start of the process
	Speed float64 `json:"speed,omitempty"`

	// an array of currently active file transfers
	Transferring []*StatsTransferringItems0 `json:"transferring"`

	// number of transferred files
	Transfers int64 `json:"transfers,omitempty"`
}

// Validate validates this stats
func (m *Stats) Validate(formats strfmt.Registry) error {
	var res []error

	if err := m.validateTransferring(formats); err != nil {
		res = append(res, err)
	}

	if len(res) > 0 {
		return errors.CompositeValidationError(res...)
	}
	return nil
}

func (m *Stats) validateTransferring(formats strfmt.Registry) error {

	if swag.IsZero(m.Transferring) { // not required
		return nil
	}

	for i := 0; i < len(m.Transferring); i++ {
		if swag.IsZero(m.Transferring[i]) { // not required
			continue
		}

		if m.Transferring[i] != nil {
			if err := m.Transferring[i].Validate(formats); err != nil {
				if ve, ok := err.(*errors.Validation); ok {
					return ve.ValidateName("transferring" + "." + strconv.Itoa(i))
				}
				return err
			}
		}

	}

	return nil
}

// MarshalBinary interface implementation
func (m *Stats) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *Stats) UnmarshalBinary(b []byte) error {
	var res Stats
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}

// StatsTransferringItems0 stats transferring items0
//
// swagger:model StatsTransferringItems0
type StatsTransferringItems0 struct {

	// total transferred bytes for this file
	Bytes int64 `json:"bytes,omitempty"`

	// estimated time in seconds until file transfer completion
	Eta float64 `json:"eta,omitempty"`

	// name of the file
	Name string `json:"name,omitempty"`

	// progress of the file transfer in percent
	Percentage float64 `json:"percentage,omitempty"`

	// size of the file in bytes
	Size int64 `json:"size,omitempty"`

	// speed in bytes/sec
	Speed float64 `json:"speed,omitempty"`

	// speed in bytes/sec as an exponentially weighted moving average
	SpeedAvg float64 `json:"speedAvg,omitempty"`
}

// Validate validates this stats transferring items0
func (m *StatsTransferringItems0) Validate(formats strfmt.Registry) error {
	return nil
}

// MarshalBinary interface implementation
func (m *StatsTransferringItems0) MarshalBinary() ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	return swag.WriteJSON(m)
}

// UnmarshalBinary interface implementation
func (m *StatsTransferringItems0) UnmarshalBinary(b []byte) error {
	var res StatsTransferringItems0
	if err := swag.ReadJSON(b, &res); err != nil {
		return err
	}
	*m = res
	return nil
}
