// Copyright (C) 2017 ScyllaDB

package rcserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/cache"
	"github.com/rclone/rclone/fs/rc"
	"github.com/scylladb/scylla-manager/v3/pkg/rclone"
	"github.com/scylladb/scylla-manager/v3/pkg/rclone/rcserver/internal"
)

func newTestServer() Server {
	rclone.InitFsConfig()
	filterRcCallsForTests()
	return New()
}

func filterRcCallsForTests() {
	internal.RcloneSupportedCalls.Add(
		"rc/noop",
		"rc/error",
	)
	filterRcCalls()
}

// Run a suite of tests
func testServer(t *testing.T, tests []httpTest) {
	t.Helper()

	rcServer := newTestServer()

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
		Expected: `{"message":"Not found","status":404}
`,
	}, {
		Name:     "rc-noop",
		URL:      "rc/noop",
		Method:   "POST",
		Status:   http.StatusOK,
		Expected: "{}\n",
	}, {
		Name:     "rc-error",
		URL:      "rc/error",
		Method:   "POST",
		Status:   http.StatusInternalServerError,
		Contains: regexp.MustCompile(`\"arbitrary error on input map\[.*`),
	}, {
		Name:   "url-params",
		URL:    "rc/noop?param1=potato&param2=sausage",
		Method: "POST",
		Status: http.StatusOK,
		Expected: `{"param1":"potato","param2":"sausage"}
`,
	}, {
		Name:        "json",
		URL:         "rc/noop",
		Method:      "POST",
		Body:        `{"param1":"string","param2":true}`,
		ContentType: "application/json",
		Status:      http.StatusOK,
		Expected: `{"param1":"string","param2":true}
`,
	}, {
		Name:        "json-and-url-params",
		URL:         "rc/noop?param1=potato&param2=sausage",
		Method:      "POST",
		Body:        `{ "param1":"string","param3":true }`,
		ContentType: "application/json",
		Status:      http.StatusOK,
		Expected: `{"param1":"string","param2":"sausage","param3":true}
`,
	}, {
		Name:        "json-bad",
		URL:         "rc/noop?param1=potato&param2=sausage",
		Method:      "POST",
		Body:        `{ param1":"string","param3":true }`,
		ContentType: "application/json",
		Status:      http.StatusBadRequest,
		Expected: `{"input":{"param1":"potato","param2":"sausage"},"message":"read input JSON: invalid character 'p' looking for beginning of object key string","path":"rc/noop","status":400}
`,
	}, {
		Name:        "form",
		URL:         "rc/noop",
		Method:      "POST",
		Body:        `param1=string&param2=true`,
		ContentType: "application/x-www-form-urlencoded",
		Status:      http.StatusOK,
		Expected: `{"param1":"string","param2":"true"}
`,
	}, {
		Name:        "form-and-url-params",
		URL:         "rc/noop?param1=potato&param2=sausage",
		Method:      "POST",
		Body:        `param1=string&param3=true`,
		ContentType: "application/x-www-form-urlencoded",
		Status:      http.StatusOK,
		Expected: `{"param1":"potato","param2":"sausage","param3":"true"}
`,
	}, {
		Name:        "form-bad",
		URL:         "rc/noop?param1=potato&param2=sausage",
		Method:      "POST",
		Body:        `%zz`,
		ContentType: "application/x-www-form-urlencoded",
		Status:      http.StatusBadRequest,
		Expected: `{"input":null,"message":"parse form/URL parameters: invalid URL escape \"%zz\"","path":"rc/noop","status":400}
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
		Expected: `{"input":null,"message":"method \"POTATO\" not allowed","path":"","status":405}
`,
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
		Contains:    regexp.MustCompile(`(?s).*\"jobid\".*`),
	}, {
		Name:        "bad",
		URL:         "rc/noop",
		Method:      "POST",
		ContentType: "application/json",
		Body:        `{ "_async":"truthy" }`,
		Status:      http.StatusBadRequest,
		Expected: `{"input":{"_async":"truthy"},"message":"couldn't parse key \"_async\" (truthy) as bool: strconv.ParseBool: parsing \"truthy\": invalid syntax","path":"rc/noop","status":400}
`,
	}}

	testServer(t, tests)
}

func TestRCAsyncSuccessJobInfo(t *testing.T) {
	rcServer := newTestServer()

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
		URL:         "job/info",
		Method:      "POST",
		ContentType: "application/json",
		Body:        body,
		Status:      http.StatusOK,
		Contains:    regexp.MustCompile("\"finished\":true.*\"success\":true.*"),
	})

	runHTTPTest(t, rcServer, httpTest{
		Name:        "error_status",
		URL:         "job/info",
		Method:      "POST",
		ContentType: "application/json",
		Body:        `{}`,
		Status:      http.StatusOK,
		Contains:    regexp.MustCompile("\"transfers\":0.*"),
	})
}

func TestRCAsyncErrorJobInfo(t *testing.T) {
	rcServer := newTestServer()

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
		URL:         "job/info",
		Method:      "POST",
		ContentType: "application/json",
		Body:        body,
		Status:      http.StatusOK,
		Contains:    regexp.MustCompile("\"finished\":true.*\"success\":false.*"),
	})

	runHTTPTest(t, rcServer, httpTest{
		Name:        "error_status",
		URL:         "job/info",
		Method:      "POST",
		ContentType: "application/json",
		Body:        `{}`,
		Status:      http.StatusOK,
		Contains:    regexp.MustCompile("\"job\":null.*\"transfers\":0.*"),
	})
}

func TestRCAsyncSuccessJobInfoStatus(t *testing.T) {
	rcServer := newTestServer()

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
		URL:         "job/info",
		Method:      "POST",
		ContentType: "application/json",
		Body:        body,
		Status:      http.StatusOK,
		Contains:    regexp.MustCompile("\"finished\":true.*\"success\":true.*"),
	})

	runHTTPTest(t, rcServer, httpTest{
		Name:        "error_status",
		URL:         "job/info",
		Method:      "POST",
		ContentType: "application/json",
		Body:        `{}`,
		Status:      http.StatusOK,
		Contains:    regexp.MustCompile("\"job\":null.*\"transfers\":0.*"),
	})
}

func TestLongBodyReturnsContentLengthHeader(t *testing.T) {
	rcServer := New()

	calls := rc.Calls.List()
	defer func() {
		rc.Calls = rc.NewRegistry()
		for _, c := range calls {
			rc.Calls.Add(*c)
		}
	}()

	rc.Calls = rc.NewRegistry()
	rc.Add(rc.Call{
		Path: "long/body",
		Fn: func(ctx context.Context, in rc.Params) (out rc.Params, err error) {
			out = make(rc.Params)
			lorem := `Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec tincidunt posuere elit et faucibus.`
			for i := 0; i < 1000; i++ {
				out[fmt.Sprintf("%d", i)] = lorem
			}
			return out, nil
		},
	})

	req := httptest.NewRequest(http.MethodPost, "http://1.2.3.4/long/body", nil)
	w := httptest.NewRecorder()
	rcServer.ServeHTTP(w, req)
	if w.Header().Get("Content-Length") == "" {
		t.Fatal("expected to have Content-Length header, got", w.Header())
	}
}

func TestOperationsList(t *testing.T) {
	const operationPath = "./testdata/list_operation"

	dirName := func(count int) string {
		return path.Join(operationPath, "file_count_"+strconv.Itoa(count))
	}

	// Setup file counts for listing
	fileCounts := []int{
		1,
		defaultListEncoderMaxItems,
		defaultListEncoderMaxItems + 1,
		2 * defaultListEncoderMaxItems,
	}

	for _, count := range fileCounts {
		if err := os.MkdirAll(dirName(count), 0o755); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < count; i++ {
			if err := os.WriteFile(path.Join(dirName(count), "file"+strconv.Itoa(i)), nil, 0o755); err != nil {
				t.Fatal(err)
			}
		}
	}
	defer os.RemoveAll(operationPath)

	rclone.InitFsConfig()
	if err := rclone.RegisterLocalDirProvider("testing", "testing provider", operationPath); err != nil {
		t.Fatal(err)
	}

	rcServer := New()

	for _, count := range fileCounts {
		buf := bytes.NewBuffer(nil)
		json.NewEncoder(buf).Encode(map[string]interface{}{
			"fs":     "testing:file_count_" + strconv.Itoa(count),
			"remote": "",
		})
		req := httptest.NewRequest(http.MethodPost, "http://1.2.3.4/operations/list", buf)
		req.Header.Add("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		rcServer.ServeHTTP(rec, req)

		v := struct {
			List []map[string]interface{} `json:"list"`
		}{}
		if err := json.NewDecoder(rec.Body).Decode(&v); err != nil {
			t.Fatal(err)
		}
		if len(v.List) != count {
			t.Errorf("Expected %d items, got %d", count, len(v.List))
			t.Log(v.List)
		}
	}
}

// TestOperationsListFsCache documents how selecting fs and remote during listing
// affects rclone permanent fs cache. More precisely, it that fs is the cache key,
// so that listing different remotes in the same fs does not grow the cache.
func TestOperationsListFsCache(t *testing.T) {
	root := t.TempDir()
	for _, d := range []string{"as_remote_a", "as_remote_b", "as_fs"} {
		if err := os.MkdirAll(path.Join(root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}

	rclone.InitFsConfig()
	if err := rclone.RegisterLocalDirProvider("fscache", "testing provider", root); err != nil {
		t.Fatal(err)
	}

	rcServer := New()
	list := func(t *testing.T, fs, remote string) {
		t.Helper()
		buf := bytes.NewBuffer(nil)
		err := json.NewEncoder(buf).Encode(map[string]any{
			"fs":     fs,
			"remote": remote,
		})
		if err != nil {
			t.Error(err)
		}
		req := httptest.NewRequest(http.MethodPost, "http://1.2.3.4/operations/list", buf)
		req.Header.Add("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		rcServer.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("list %q %q: status %d body %s", fs, remote, rec.Code, rec.Body.String())
		}
	}
	// cached reports whether file system fsString is in the cache.
	// The create function is called only on a miss (and its error is not cached).
	cached := func(fsString string) bool {
		miss := false
		_, _ = cache.GetFn(t.Context(), fsString, func(context.Context, string) (fs.Fs, error) {
			miss = true
			return nil, fmt.Errorf("probe")
		})
		return !miss
	}

	list(t, "fscache:", "as_remote_a")
	list(t, "fscache:", "as_remote_b")
	list(t, "fscache:as_fs", "")

	if !cached("fscache:") {
		t.Error("expected fs root to be cached after listing directories as remote")
	}
	for _, d := range []string{"fscache:as_remote_a", "fscache:as_remote_b"} {
		if cached(d) {
			t.Errorf("expected %q not to be cached when listed as remote", d)
		}
	}
	if !cached("fscache:as_fs") {
		t.Error("expected directory listed as fs to be cached")
	}
}

// TestRemoteEscapingRootRejected verifies that a remote path escaping
// fs root is rejected before reaching the backend.
func TestRemoteEscapingRootRejected(t *testing.T) {
	root := t.TempDir()
	jail := path.Join(root, "jail")
	if err := os.MkdirAll(path.Join(jail, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{path.Join(root, "outside.txt"), path.Join(jail, "inside.txt")} {
		if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	rclone.InitFsConfig()
	if err := rclone.RegisterLocalDirProvider("escape", "testing provider", jail); err != nil {
		t.Fatal(err)
	}

	rcServer := New()
	call := func(t *testing.T, endpoint string, in map[string]any) (int, string) {
		t.Helper()
		buf := bytes.NewBuffer(nil)
		if err := json.NewEncoder(buf).Encode(in); err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "http://1.2.3.4/"+endpoint, buf)
		req.Header.Add("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		rcServer.ServeHTTP(rec, req)
		return rec.Code, rec.Body.String()
	}

	testCases := []struct {
		Name     string
		Endpoint string
		In       map[string]any
		Code     int
	}{
		{
			Name:     "list parent of fs root",
			Endpoint: "operations/list",
			In:       map[string]any{"fs": "escape:sub", "remote": ".."},
			Code:     http.StatusBadRequest,
		},
		{
			Name:     "list above jail",
			Endpoint: "operations/list",
			In:       map[string]any{"fs": "escape:sub", "remote": "../.."},
			Code:     http.StatusBadRequest,
		},
		{
			Name:     "fileinfo above jail",
			Endpoint: "operations/fileinfo",
			In:       map[string]any{"fs": "escape:", "remote": "../outside.txt"},
			Code:     http.StatusBadRequest,
		},
		{
			Name:     "deletepaths above jail",
			Endpoint: "operations/deletepaths",
			In:       map[string]any{"fs": "escape:", "remote": "sub", "paths": []string{"../../outside.txt"}},
			Code:     http.StatusBadRequest,
		},
		{
			Name:     "list within fs root",
			Endpoint: "operations/list",
			In:       map[string]any{"fs": "escape:", "remote": "sub/.."},
			Code:     http.StatusOK,
		},
		{
			Name:     "fileinfo within jail",
			Endpoint: "operations/fileinfo",
			In:       map[string]any{"fs": "escape:", "remote": "inside.txt"},
			Code:     http.StatusOK,
		},
		{
			Name:     "fs above jail is still clamped by the provider",
			Endpoint: "operations/list",
			In:       map[string]any{"fs": "escape:sub/../..", "remote": ""},
			Code:     http.StatusNotFound,
		},
	}

	for _, test := range testCases {
		t.Run(test.Name, func(t *testing.T) {
			code, body := call(t, test.Endpoint, test.In)
			if code != test.Code {
				t.Fatalf("status %d, expected %d, body %s", code, test.Code, body)
			}
			if code == http.StatusOK && strings.Contains(body, "outside.txt") {
				t.Fatalf("response leaks content above jail: %s", body)
			}
		})
	}

	if _, err := os.Stat(path.Join(root, "outside.txt")); err != nil {
		t.Fatalf("file above jail was removed: %s", err)
	}
}

func TestOperationsListPermissionError(t *testing.T) {
	tmpDir, err := ioutil.TempDir("", "scylla-manager-agent-")
	if err != nil {
		t.Fatal(err, "create local tmp directory")
	}
	defer os.RemoveAll(tmpDir)

	secretDir := "secret"

	// No read permission, only write needed for deletion
	if err := os.Mkdir(path.Join(tmpDir, secretDir), 0o222); err != nil {
		log.Fatal(err)
	}

	rclone.InitFsConfig()
	if err := rclone.RegisterLocalDirProvider("noperm", "testing provider", tmpDir); err != nil {
		t.Fatal(err)
	}

	rcServer := New()

	buf := bytes.NewBuffer(nil)
	json.NewEncoder(buf).Encode(map[string]interface{}{
		"fs":     "noperm:" + secretDir,
		"remote": "",
	})
	req := httptest.NewRequest(http.MethodPost, "http://1.2.3.4/operations/list", buf)
	req.Header.Add("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	rcServer.ServeHTTP(rec, req)

	if rec.Result().StatusCode != http.StatusForbidden {
		t.Errorf("Expected %d status code, got %d", http.StatusForbidden, rec.Result().StatusCode)
	}
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
	body, err := io.ReadAll(resp.Body)
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
