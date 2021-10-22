// Copyright (C) 2017 ScyllaDB

package httpexec

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/scylladb/go-log"
)

func TestServer(t *testing.T) {
	pr, pw := io.Pipe()
	defer pr.Close()
	defer pw.Close()

	go func() {
		fmt.Fprintf(pw, "echo 1\n\n")
		time.Sleep(time.Second)
		fmt.Fprintln(pw, "echo 2")
		pw.Close()
	}()

	r := httptest.NewRequest(http.MethodPost, "/", pr)
	w := httptest.NewRecorder()

	s := NewBashServer(log.NewDevelopment())
	s.ServeHTTP(w, r)
	fmt.Println(w.Body.String())
}

