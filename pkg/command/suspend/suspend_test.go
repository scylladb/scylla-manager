package suspend

import (
	"testing"

	"github.com/scylladb/scylla-manager/pkg/util/duration"
)

func Test_ValidateSuspend(t *testing.T) {
	testCases := []struct {
		name  string
		props map[string]interface{}
		valid bool
	}{
		{
			name: "start with string",
			props: map[string]interface{}{
				"start_tasks": true,
				"duration":    "5m",
			},
			valid: true,
		},
		{
			name: "start with duration",
			props: map[string]interface{}{
				"start_tasks": true,
				"duration":    duration.Duration(5),
			},
			valid: true,
		},
		{
			name: "no start without duration",
			props: map[string]interface{}{
				"start_tasks": false,
			},
			valid: true,
		},
		{
			name: "start without duration",
			props: map[string]interface{}{
				"start_tasks": true,
			},
			valid: false,
		},
		{
			name: "start with 0 duration",
			props: map[string]interface{}{
				"start_tasks": true,
				"duration":    duration.Duration(0),
			},
			valid: false,
		},
		{
			name: "start with 0 string",
			props: map[string]interface{}{
				"start_tasks": true,
				"duration":    "0h",
			},
			valid: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateSuspend(tc.props)

			if tc.valid && err != nil {
				t.Fatal("expected valid, but got error")
			}

			if !tc.valid && err == nil {
				t.Fatal("expected invalid, but got no error")
			}
		})
	}
}
