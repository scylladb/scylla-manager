// Copyright (C) 2017 ScyllaDB

package main

import (
	"encoding/json"
	"fmt"

	"github.com/go-openapi/runtime"
)

type printableError struct {
	inner error
}

func (err printableError) Error() string {
	switch e := err.inner.(type) {
	case *runtime.APIError:
		msg, ok := e.Response.(json.RawMessage)
		if !ok {
			return fmt.Sprintf("%s (status %d)", e.OperationName, e.Code)
		}
		s := string(msg)

		// Try indent
		if b, err := json.MarshalIndent(msg, "", "  "); err == nil {
			s = string(b)
		}
		return fmt.Sprintf("%s (status %d)\n%s", e.OperationName, e.Code, s)

	default:
		return e.Error()
	}
}
