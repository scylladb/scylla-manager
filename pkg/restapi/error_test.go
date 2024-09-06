// Copyright (C) 2017 ScyllaDB
package restapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/util"
)

func TestRespondError(t *testing.T) {
	request, _ := http.NewRequest(http.MethodGet, "/", nil)

	t.Run("not found", func(t *testing.T) {
		err := errors.Wrap(gocql.ErrNotFound, "wrapped")
		response := httptest.NewRecorder()

		respondError(response, request, errors.Wrap(err, "specific_msg"))
		expected := `{"message":"get resource: specific_msg: wrapped: not found","details":"","trace_id":""}` + "\n"
		if diff := cmp.Diff(response.Body.String(), expected); diff != "" {
			t.Fatal(diff)
		}
		if response.Code != http.StatusNotFound {
			t.Errorf("Response status is wrong, got '%d' want '%d'", response.Code, http.StatusNotFound)
		}
	})

	t.Run("validation", func(t *testing.T) {
		err := util.ErrValidate(errors.New("some problem"))
		response := httptest.NewRecorder()

		respondError(response, request, errors.Wrap(err, "specific_msg"))
		expected := `{"message":"specific_msg: some problem","details":"","trace_id":""}` + "\n"
		if diff := cmp.Diff(response.Body.String(), expected); diff != "" {
			t.Fatal(diff)
		}
		if response.Code != http.StatusBadRequest {
			t.Errorf("Response status is wrong, got '%d' want '%d'", response.Code, http.StatusBadRequest)
		}
	})

	t.Run("internal errors", func(t *testing.T) {
		err := errors.Wrap(errors.New("unknown problem"), "wrapped")
		response := httptest.NewRecorder()

		respondError(response, request, errors.Wrap(err, "specific_msg"))
		expected := `{"message":"specific_msg: wrapped: unknown problem","details":"","trace_id":""}` + "\n"
		if diff := cmp.Diff(response.Body.String(), expected); diff != "" {
			t.Fatal(diff)
		}

		if response.Code != http.StatusInternalServerError {
			t.Errorf("Response status is wrong, got '%d' want '%d'", response.Code, http.StatusInternalServerError)
		}
	})
}
