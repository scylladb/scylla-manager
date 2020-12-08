// Copyright (C) 2017 ScyllaDB

package location

import (
	"regexp"

	"github.com/pkg/errors"
)

// ErrInvalid means that location is not adhering to scylla manager required
// [dc:]<provider>:<bucket> format.
var ErrInvalid = errors.Errorf("invalid location, the format is [dc:]<provider>:<bucket> ex. s3:my-bucket, the bucket name must be DNS compliant")

// Providers require that resource names are DNS compliant.
// The following is a super simplified DNS (plus provider prefix)
// matching regexp.
var pattern = regexp.MustCompile(`^(([a-zA-Z0-9\-\_\.]+):)?([a-z0-9]+):([a-z0-9\-\.]+)$`)

// Split first checks if location string conforms to valid pattern.
// It then returns the location split into three components dc, remote, and
// bucket.
func Split(location string) (dc, provider, bucket string, err error) {
	m := pattern.FindStringSubmatch(location)
	if m == nil {
		return "", "", "", ErrInvalid
	}

	return m[2], m[3], m[4], nil
}

// StripDC returns valid location string after stripping the dc prefix.
func StripDC(location string) (string, error) {
	_, provider, bucket, err := Split(location)
	if err != nil {
		return "", err
	}

	return provider + ":" + bucket, nil
}
