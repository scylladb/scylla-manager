// Copyright (C) 2017 ScyllaDB
package restapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"github.com/scylladb/mermaid"
)

func TestRespondError(t *testing.T) {
	request, _ := http.NewRequest(http.MethodGet, "/", nil)

	t.Run("not found", func(t *testing.T) {
		err := errors.Wrap(gocql.ErrNotFound, "wrapped")
		response := httptest.NewRecorder()

		respondError(response, request, err, "specific_msg")
		expected := `{"message":"resource not found: specific_msg","trace_id":""}` + "\n"
		if diff := cmp.Diff(response.Body.String(), expected); diff != "" {
			t.Fatal(diff)
		}
		if response.Code != http.StatusNotFound {
			t.Errorf("response status is wrong, got '%d' want '%d'", response.Code, http.StatusNotFound)
		}
	})

	t.Run("validation", func(t *testing.T) {
		err := mermaid.ErrValidate(errors.New("some problem"), "val_err")
		response := httptest.NewRecorder()

		respondError(response, request, err, "specific_msg")
		expected := `{"message":"val_err: some problem","trace_id":""}` + "\n"
		if diff := cmp.Diff(response.Body.String(), expected); diff != "" {
			t.Fatal(diff)
		}
		if response.Code != http.StatusBadRequest {
			t.Errorf("response status is wrong, got '%d' want '%d'", response.Code, http.StatusBadRequest)
		}
	})

	t.Run("internal errors", func(t *testing.T) {
		err := errors.Wrap(errors.New("unknown problem"), "wrapped")
		response := httptest.NewRecorder()

		respondError(response, request, err, "specific_msg")
		expected := `{"cause":"wrapped: unknown problem","message":"specific_msg","trace_id":""}` + "\n"
		if diff := cmp.Diff(response.Body.String(), expected); diff != "" {
			t.Fatal(diff)
		}

		if response.Code != http.StatusInternalServerError {
			t.Errorf("response status is wrong, got '%d' want '%d'", response.Code, http.StatusInternalServerError)
		}
	})
}
