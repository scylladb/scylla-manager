// Copyright (C) 2017 ScyllaDB

package operations

import (
	"net"
	"testing"

	"github.com/pkg/errors"
)

func TestParseAWSError(t *testing.T) {
	t.Run("valid XML", func(t *testing.T) {
		err404 := errors.New(`s3 upload: 404 Not Found: <?xml version="1.0" encoding="UTF-8"?>
	<Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message><BucketName>backuptest-rclone</BucketName><RequestId>5ZYP7GJWCJE67BDY</RequestId><HostId>254h+dP3mw7YqUJKSyfUgAPl+wxT8yLiqSgRb6Somygc/N5a/bu144Lw1bmD1bbRB0YLquP2+iY=</HostId></Error>`)

		e, err := parseAWSError(err404)
		if err != nil {
			t.Fatal(err)
		}
		t.Log(e)
	})

	t.Run("not XML", func(t *testing.T) {
		errNet := &net.AddrError{}
		_, err := parseAWSError(errNet)
		if err == nil {
			t.Fatal("expected error")
		}
		t.Log(err)
	})

	t.Run("invalid XML", func(t *testing.T) {
		errFoo := errors.New(`<?xml version="1.0" encoding="UTF-8"?><Foo></Foo>`)

		_, err := parseAWSError(errFoo)
		if err == nil {
			t.Fatal("expected error")
		}
		t.Log(err)
	})
}
