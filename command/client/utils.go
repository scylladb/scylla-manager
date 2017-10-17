// Copyright (C) 2017 ScyllaDB

package client

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-openapi/runtime"
)

// errorStr converts an error to a string representation.
func errorStr(err error) string {
	switch e := err.(type) {
	case *runtime.APIError:
		msg, ok := e.Response.(json.RawMessage)
		if !ok {
			return fmt.Sprintf("%s (status %d)", e.OperationName, e.Code)
		}
		s := string(msg)

		// try indent
		if b, err := json.MarshalIndent(msg, "", "  "); err == nil {
			s = string(b)
		}
		return fmt.Sprintf("%s (status %d)\n%s", e.OperationName, e.Code, s)
	}

	return fmt.Sprintf("Error: %s", err)
}

func parseList(s string) []string {
	var l []string
	if s != "" {
		l = strings.Split(s, ",")
	}
	return l
}
