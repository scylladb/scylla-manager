// Copyright (C) 2024 ScyllaDB

package cloudmeta

import "testing"

func TestParseMachineTypeResponse(t *testing.T) {
	testCases := []struct {
		name                string
		machineTypeResponse string

		expectedErr bool
		expected    string
	}{
		{
			name:                "everything is fine",
			machineTypeResponse: "projects/project1/machineTypes/machineType1",

			expectedErr: false,
			expected:    "machineType1",
		},
		{
			name:                "new response part is added",
			machineTypeResponse: "projects/project1/zone/zone1/machineTypes/machineType1",

			expectedErr: true,
			expected:    "",
		},
		{
			name:                "parts are mixed up",
			machineTypeResponse: "machineTypes/machineType1/projects/project1",

			expectedErr: true,
			expected:    "",
		},
		{
			name:                "empty response",
			machineTypeResponse: "",

			expectedErr: true,
			expected:    "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			machineType, err := parseMachineTypeResponse(tc.machineTypeResponse)
			if tc.expectedErr && err == nil {
				t.Fatalf("expected err, but got %v", err)
			}
			if !tc.expectedErr && err != nil {
				t.Fatalf("unexpected err: %v", err)
			}

			if tc.expected != machineType {
				t.Fatalf("machineType(%s) != expected(%s)", machineType, tc.expected)
			}
		})
	}
}
