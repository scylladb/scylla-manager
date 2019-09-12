// Copyright (C) 2017 ScyllaDB

package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestParseBearerAuth(t *testing.T) {
	table := []struct {
		Name   string
		Header string
		Token  string
	}{
		{
			Name:   "empty header",
			Header: "",
			Token:  "",
		},
		{
			Name:   "bearer canonical token",
			Header: "Bearer token",
			Token:  "token",
		},
		{
			Name:   "bearer case mismatch token",
			Header: "bEaReR token",
			Token:  "token",
		},
		{
			Name:   "basic auth",
			Header: "Basic foobar",
			Token:  "",
		},
	}

	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			if token := parseBearerAuth(test.Header); token != test.Token {
				t.Error("expected", test.Token, "got", token)
			}
		})
	}
}

func TestValidateAuthTokenMiddlewareNoToken(t *testing.T) {
	h := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	r := httptest.NewRequest(http.MethodGet, "/foobar", nil)
	w := httptest.NewRecorder()

	ValidateAuthToken(h, "", 0).ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Error("expected status 200 got", w)
	}
}

func TestValidateAuthTokenMiddlewareSuccess(t *testing.T) {
	const token = "token"
	h := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	r := httptest.NewRequest(http.MethodGet, "/foobar", nil)
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()

	ValidateAuthToken(h, token, 0).ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Error("expected status 200 got", w)
	}
}

func TestValidateAuthTokenMiddlewareFailure(t *testing.T) {
	h := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("this must not be called")
	})

	verify := func(t *testing.T, r *http.Request, penalty time.Duration) {
		t.Helper()

		w := httptest.NewRecorder()
		ValidateAuthToken(h, "token", penalty).ServeHTTP(w, r)
		if w.Code != http.StatusUnauthorized {
			t.Error("expected status 401 got", w)
		}
	}

	t.Run("no token", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/foobar", nil)
		verify(t, r, 0)
	})

	t.Run("invalid token", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/foobar", nil)
		r.Header.Set("Authorization", "Bearer foobar")
		verify(t, r, 0)
	})

	t.Run("penalty", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/foobar", nil)
		penalty := 150 * time.Millisecond
		start := time.Now()
		verify(t, r, penalty)
		if time.Since(start) < penalty {
			t.Fatal("expected penalty")
		}
	})
}

func TestCrossCheckAuthTokenMiddleware(t *testing.T) {
	const token = "token"

	var h http.Handler
	h = http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	h = ValidateAuthToken(h, token, 0)

	var rt http.RoundTripper
	rt = RoundTripperFunc(func(r *http.Request) (*http.Response, error) {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Result(), nil
	})
	rt = AuthToken(rt, token)

	req := httptest.NewRequest(http.MethodGet, "/foobar", nil)
	resp, _ := rt.RoundTrip(req)
	if resp.StatusCode != http.StatusOK {
		t.Error("expected status 200 got", resp.StatusCode)
	}
}
