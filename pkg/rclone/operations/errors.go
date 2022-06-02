// Copyright (C) 2017 ScyllaDB

package operations

import (
	"encoding/xml"
	"errors"
	"strings"
)

// AWSError is error parsed from AWS Error XML message as specified in
// https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList
type AWSError struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

// parseAWSError reads the error as string and ties to parse the XML structure.
// The reason is that the error returned from rclone is flattened ex. `*errors.fundamental s3 upload: 404 Not Found: <?xml version="1.0" encoding="UTF-8"?>`.
func parseAWSError(err error) (*AWSError, error) {
	s := err.Error()
	if idx := strings.Index(s, "<?xml"); idx > 0 {
		s = s[idx:]
	} else if idx < 0 {
		return nil, errors.New("not XML")
	}

	var e AWSError
	if err := xml.Unmarshal([]byte(s), &e); err != nil {
		return nil, err
	}
	return &e, nil
}

func (e *AWSError) Error() string {
	return e.Message + " (code:" + e.Code + ")"
}
