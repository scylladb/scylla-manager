// Copyright (C) 2017 ScyllaDB

package httpshell

import (
	"bufio"
	"bytes"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestHTTPShell(t *testing.T) {
	httpReq1 := `GET /foo HTTP/1.1
User-Agent: TestAgent
Host: www.scylladb.com
Accept-Encoding: gzip

`
	httpReq2 := `GET /bar HTTP/1.1
User-Agent: TestAgent
Host: www.google.com
Accept-Encoding: gzip

`
	var wg sync.WaitGroup

	exp := []expected{
		{
			req:  makeTestRequest("/foo", "www.scylladb.com"),
			resp: resp{http.StatusOK, http.Header{}, nil},
		},
		{
			req:  makeTestRequest("/bar", "www.google.com"),
			resp: resp{http.StatusOK, http.Header{}, nil},
		},
	}
	mh, close := newMockHandler(t, exp)
	defer close()

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()

	defer func() {
		inW.Close()
		outW.Close()
		wg.Wait()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := Serve(inR, outW, mh); err != nil {
			t.Error(err.Error())
		}
	}()

	if _, err := io.WriteString(inW, httpReq1); err != nil {
		t.Fatal(err.Error())
	}

	res := readResponse(outR)

	if !strings.HasPrefix(res, "HTTP/1.1 200 OK") || !strings.HasSuffix(res, " charset=utf-8\r\n\r\n") {
		t.Errorf("HTTP response not expected, got:\n%q\n", res)
	}

	if _, err := io.WriteString(inW, httpReq2); err != nil {
		t.Fatal(err.Error())
	}

	res = readResponse(outR)

	if !strings.HasPrefix(res, "HTTP/1.1 200 OK") || !strings.HasSuffix(res, " charset=utf-8\r\n\r\n") {
		t.Errorf("HTTP response not expected, got:\n%q\n", res)
	}

	inW.Write([]byte{})

}

func makeTestRequest(url, host string) *http.Request {
	return &http.Request{
		Method:     http.MethodGet,
		Host:       host,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		URL:        mustURL(url),
		RequestURI: mustURL(url).Path,
		Body:       http.NoBody,
		Header:     http.Header{"User-Agent": []string{"TestAgent"}, "Accept-Encoding": []string{"gzip"}},
	}
}

func readResponse(r io.Reader) string {
	br := bufio.NewReader(r)
	buf := bytes.NewBuffer(nil)
	for {
		b, err := br.ReadBytes('\n')
		if err != nil {
			panic(err.Error())
		}
		buf.Write(b)
		if len(bytes.TrimSpace(b)) == 0 {
			break
		}
	}

	return buf.String()
}

func mustURL(urlS string) *url.URL {
	u, err := url.Parse(urlS)
	if err != nil {
		panic(err.Error())
	}
	return u
}

type resp struct {
	code   int
	header http.Header
	out    []byte
}

type expected struct {
	req  *http.Request
	resp resp
}

type mockHandler struct {
	t *testing.T

	mu       sync.Mutex
	EXPECTED []expected
	count    int
}

func newMockHandler(t *testing.T, exp []expected) (*mockHandler, func()) {
	h := &mockHandler{
		t:        t,
		EXPECTED: exp,
	}
	return h, func() {
		if h.count != len(h.EXPECTED) {
			t.Errorf("Not all expected requests were received, missing:\n%+v\n", h.EXPECTED[h.count:])
		}
	}
}

func (h *mockHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.mu.Lock()
	defer func() {
		h.count++
		h.mu.Unlock()
	}()

	if h.count > len(h.EXPECTED) {
		h.t.Errorf("Unexpected call to mock server: \n%+v\n", r)
		return
	}

	if diff := cmp.Diff(h.EXPECTED[h.count].req, r, cmpopts.IgnoreFields(http.Request{}, "RemoteAddr"), cmpopts.IgnoreUnexported(http.Request{})); diff != "" {
		h.t.Error(diff)
	}

	for key, val := range h.EXPECTED[h.count].resp.header {
		w.Header().Set(key, val[0])
	}

	w.Write(h.EXPECTED[h.count].resp.out)
}
