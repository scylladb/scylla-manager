// Copyright (C) 2017 ScyllaDB

package httpexec

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/scylladb/go-log"
)

func TestServer(t *testing.T) {
	var buf bytes.Buffer
	buf.WriteString(`xxx || : && echo "foo"
echo "bar"
`)

	r := httptest.NewRequest(http.MethodPost, "/", &buf)
	w := httptest.NewRecorder()

	s := NewBashServer(log.NewDevelopment())
	s.ServeHTTP(w, r)
	fmt.Println(w.Body.String())
}
