// Copyright (C) 2017 ScyllaDB

package scyllaclienttest

import (
	"fmt"
	"net/http"
)

// Matcher defines a function used to determine the file to return from a given newMockServer call.
type Matcher func(req *http.Request) string

// FileMatcher is a simple matcher created for backwards compatibility.
func FileMatcher(file string) Matcher {
	return func(req *http.Request) string {
		return file
	}
}

// PathFileMatcher matcher which verifies URL path.
func PathFileMatcher(path, file string) Matcher {
	return func(req *http.Request) string {
		if req.URL.Path == path {
			return file
		}
		return ""
	}
}

// MultiPathFileMatcher accepts multiple PathFile matchers and returns result of
// first successful match.
func MultiPathFileMatcher(matchers ...Matcher) Matcher {
	return func(req *http.Request) string {
		for _, m := range matchers {
			f := m(req)
			if f != "" {
				return f
			}
		}
		panic(fmt.Errorf("not found matcher registered for %s path", req.URL.Path))
	}
}
