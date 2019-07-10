// Copyright (C) 2017 ScyllaDB

package rcserver

import (
	"bytes"
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
	"time"
)

// Run a suite of tests
func testServer(t *testing.T, tests []httpTest) {
	t.Helper()
	registerInMemoryConf()
	rcServer := New()
	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			runHTTPTest(t, rcServer, test)
			time.Sleep(5 * time.Millisecond)
		})
	}
}

func TestRC(t *testing.T) {
	tests := []httpTest{{
		Name:   "rc-root",
		URL:    "",
		Method: "POST",
		Status: http.StatusNotFound,
		Expected: `{
	"error": "couldn't find method \"\"",
	"input": {},
	"path": "",
	"status": 404
}
`,
	}, {
		Name:     "rc-noop",
		URL:      "rc/noop",
		Method:   "POST",
		Status:   http.StatusOK,
		Expected: "{}\n",
	}, {
		Name:   "rc-error",
		URL:    "rc/error",
		Method: "POST",
		Status: http.StatusInternalServerError,
		Expected: `{
	"error": "arbitrary error on input map[]",
	"input": {},
	"path": "rc/error",
	"status": 500
}
`,
	}, {
		Name:     "core-gc",
		URL:      "core/gc", // returns nil, nil so check it is made into {}
		Method:   "POST",
		Status:   http.StatusOK,
		Expected: "{}\n",
	}, {
		Name:   "url-params",
		URL:    "rc/noop?param1=potato&param2=sausage",
		Method: "POST",
		Status: http.StatusOK,
		Expected: `{
	"param1": "potato",
	"param2": "sausage"
}
`,
	}, {
		Name:        "json",
		URL:         "rc/noop",
		Method:      "POST",
		Body:        `{ "param1":"string", "param2":true }`,
		ContentType: "application/json",
		Status:      http.StatusOK,
		Expected: `{
	"param1": "string",
	"param2": true
}
`,
	}, {
		Name:        "json-and-url-params",
		URL:         "rc/noop?param1=potato&param2=sausage",
		Method:      "POST",
		Body:        `{ "param1":"string", "param3":true }`,
		ContentType: "application/json",
		Status:      http.StatusOK,
		Expected: `{
	"param1": "string",
	"param2": "sausage",
	"param3": true
}
`,
	}, {
		Name:        "json-bad",
		URL:         "rc/noop?param1=potato&param2=sausage",
		Method:      "POST",
		Body:        `{ param1":"string", "param3":true }`,
		ContentType: "application/json",
		Status:      http.StatusBadRequest,
		Expected: `{
	"error": "failed to read input JSON: invalid character 'p' looking for beginning of object key string",
	"input": {
		"param1": "potato",
		"param2": "sausage"
	},
	"path": "rc/noop",
	"status": 400
}
`,
	}, {
		Name:        "form",
		URL:         "rc/noop",
		Method:      "POST",
		Body:        `param1=string&param2=true`,
		ContentType: "application/x-www-form-urlencoded",
		Status:      http.StatusOK,
		Expected: `{
	"param1": "string",
	"param2": "true"
}
`,
	}, {
		Name:        "form-and-url-params",
		URL:         "rc/noop?param1=potato&param2=sausage",
		Method:      "POST",
		Body:        `param1=string&param3=true`,
		ContentType: "application/x-www-form-urlencoded",
		Status:      http.StatusOK,
		Expected: `{
	"param1": "potato",
	"param2": "sausage",
	"param3": "true"
}
`,
	}, {
		Name:        "form-bad",
		URL:         "rc/noop?param1=potato&param2=sausage",
		Method:      "POST",
		Body:        `%zz`,
		ContentType: "application/x-www-form-urlencoded",
		Status:      http.StatusBadRequest,
		Expected: `{
	"error": "failed to parse form/URL parameters: invalid URL escape \"%zz\"",
	"input": null,
	"path": "rc/noop",
	"status": 400
}
`,
	}}

	testServer(t, tests)
}

func TestMethods(t *testing.T) {
	tests := []httpTest{{
		Name:   "bad",
		URL:    "",
		Method: "POTATO",
		Status: http.StatusMethodNotAllowed,
		Expected: `{
	"error": "method \"POTATO\" not allowed",
	"input": null,
	"path": "",
	"status": 405
}
`,
	}}

	testServer(t, tests)
}

func TestNoFiles(t *testing.T) {
	tests := []httpTest{{
		Name:     "file",
		URL:      "file.txt",
		Status:   http.StatusNotFound,
		Expected: "Not Found\n",
	}, {
		Name:     "dir",
		URL:      "dir/",
		Status:   http.StatusNotFound,
		Expected: "Not Found\n",
	}}

	testServer(t, tests)
}

func TestNoServe(t *testing.T) {
	tests := []httpTest{{
		Name:     "file",
		URL:      "/file.txt",
		Status:   http.StatusNotFound,
		Expected: "Not Found\n",
	}, {
		Name:     "dir",
		URL:      "/dir/",
		Status:   http.StatusNotFound,
		Expected: "Not Found\n",
	}}

	testServer(t, tests)
}

func TestRCAsync(t *testing.T) {
	tests := []httpTest{{
		Name:        "ok",
		URL:         "rc/noop",
		Method:      "POST",
		ContentType: "application/json",
		Body:        `{ "_async":true }`,
		Status:      http.StatusOK,
		Contains: regexp.MustCompile(`(?s).*\"jobid\".*`),
	}, {
		Name:        "bad",
		URL:         "rc/noop",
		Method:      "POST",
		ContentType: "application/json",
		Body:        `{ "_async":"truthy" }`,
		Status:      http.StatusBadRequest,
		Expected: `{
	"error": "couldn't parse key \"_async\" (truthy) as bool: strconv.ParseBool: parsing \"truthy\": invalid syntax",
	"input": {
		"_async": "truthy"
	},
	"path": "rc/noop",
	"status": 400
}
`,
	}}

	testServer(t, tests)
}

func TestRCAsyncSuccessStatus(t *testing.T) {
	registerInMemoryConf()
	rcServer := New()

	body := runHTTPTest(t, rcServer, httpTest{
		Name:        "ok",
		URL:         "rc/noop",
		Method:      "POST",
		ContentType: "application/json",
		Body:        `{ "_async":true }`,
		Status:      http.StatusOK,
	})

	time.Sleep(10 * time.Millisecond)

	runHTTPTest(t, rcServer, httpTest{
		Name:        "error_status",
		URL:         "job/status",
		Method:      "POST",
		ContentType: "application/json",
		Body:        body,
		Status:      http.StatusOK,
		Contains:    regexp.MustCompile("(?si).*\"finished\": true.*\"success\": true.*"),
	})
}

func TestRCAsyncErrorStatus(t *testing.T) {
	registerInMemoryConf()
	rcServer := New()

	body := runHTTPTest(t, rcServer, httpTest{
		Name:        "ok",
		URL:         "rc/error",
		Method:      "POST",
		ContentType: "application/json",
		Body:        `{ "_async":true }`,
		Status:      http.StatusOK,
	})

	time.Sleep(10 * time.Millisecond)

	runHTTPTest(t, rcServer, httpTest{
		Name:        "error_status",
		URL:         "job/status",
		Method:      "POST",
		ContentType: "application/json",
		Body:        body,
		Status:      http.StatusOK,
		Contains:    regexp.MustCompile("(?si).*\"finished\": true.*\"success\": false.*"),
	})
}

func TestRCExcludeParams(t *testing.T) {
	registerInMemoryConf()
	rcServer := New()

	runHTTPTest(t, rcServer, httpTest{
		Name:        "ok",
		URL:         "rc/noop",
		Method:      "POST",
		ContentType: "application/json",
		Body:        `{ "exclude": ["test1", "test2"] }`,
		Status:      http.StatusOK,
	})
}

// httpTest specifies expected request response cycle behavior needed to test
// http handlers.
type httpTest struct {
	Name        string
	URL         string
	Status      int
	Method      string
	Range       string
	Body        string
	ContentType string
	Expected    string
	Contains    *regexp.Regexp
	Headers     map[string]string
}

// runHTTPTest executes request on handler and does matching based on httpTest
// specification.
// Returns response body.
func runHTTPTest(t *testing.T, s http.Handler, test httpTest) string {
	t.Helper()
	method := test.Method
	if method == "" {
		method = "GET"
	}
	var inBody io.Reader
	if test.Body != "" {
		buf := bytes.NewBufferString(test.Body)
		inBody = buf
	}
	req, err := http.NewRequest(method, "http://1.2.3.4/"+test.URL, inBody)
	if err != nil {
		t.Fatal(err.Error())
	}
	if test.Range != "" {
		req.Header.Add("Range", test.Range)
	}
	if test.ContentType != "" {
		req.Header.Add("Content-Type", test.ContentType)
	}

	w := httptest.NewRecorder()
	s.ServeHTTP(w, req)
	resp := w.Result()

	if test.Status != resp.StatusCode {
		t.Errorf("Expected status code %d, got %d", test.Status, resp.StatusCode)
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err.Error())
	}

	if test.Contains == nil && test.Expected != "" {
		if test.Expected != string(body) {
			t.Errorf("Expected body:\n%q\ngot:\n%q", test.Expected, string(body))
		}
	} else if test.Contains != nil {
		if !test.Contains.Match(body) {
			t.Errorf("Body didn't match:\n%v\ngot:\n%q", test.Contains, string(body))
		}
	}

	for k, v := range test.Headers {
		if v != resp.Header.Get(k) {
			t.Errorf("Expected header %q:%q, got %q:%q", k, v, k, resp.Header.Get(k))
		}
	}
	return string(body)
}
