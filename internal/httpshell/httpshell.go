// Copyright (C) 2017 ScyllaDB

package httpshell

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"strings"
)

type responseWriter struct {
	headerMap   http.Header
	w           *bufio.Writer
	wroteHeader bool
}

// Header returns the response headers.
func (rw *responseWriter) Header() http.Header {
	m := rw.headerMap
	if m == nil {
		m = make(http.Header)
		rw.headerMap = m
	}
	return m
}

// Write writes to writer.
func (rw *responseWriter) Write(buf []byte) (int, error) {
	rw.writeHeader(buf, "")
	if rw.w != nil {
		n, err := rw.w.Write(buf)
		if err != nil {
			return n, err
		}
		return n, rw.w.Flush()
	}
	return len(buf), nil
}

// writeHeader writes a header if it was not written yet and
// detects Content-Type if needed.
//
// bytes or str are the beginning of the response body.
// We pass both to avoid unnecessarily generate garbage
// in rw.WriteString which was created for performance reasons.
// Non-nil bytes win.
func (rw *responseWriter) writeHeader(b []byte, str string) {
	if rw.wroteHeader {
		return
	}
	if len(str) > 512 {
		str = str[:512]
	}

	m := rw.Header()

	_, hasType := m["Content-Type"]
	hasTE := m.Get("Transfer-Encoding") != ""
	if !hasType && !hasTE {
		if b == nil {
			b = []byte(str)
		}
		m.Set("Content-Type", http.DetectContentType(b))
	}

	rw.WriteHeader(200)
}

// WriteHeader sends status line along with all already set headers.
func (rw *responseWriter) WriteHeader(code int) {
	if rw.wroteHeader {
		return
	}
	rw.wroteHeader = true
	if rw.headerMap == nil {
		rw.headerMap = make(http.Header)
	}
	statusHeader := fmt.Sprintf("HTTP/1.1 %03d %s\r\n", code, http.StatusText(code))
	rw.w.WriteString(statusHeader) //nolint:errcheck
	// Doesn't support trailer headers.
	for k, v := range rw.Header() {
		rw.w.WriteString(fmt.Sprintf("%s: %s\r\n", http.CanonicalHeaderKey(k), strings.Join(v, ","))) //nolint:errcheck
	}
	rw.w.WriteString("\r\n") //nolint:errcheck
}

// Serve is taking HTTP requests from the input reader, processes them with
// handler, and sends responses to the output writer.
// Serve is blocking until input is closed or returns an error.
// io.EOF are treated as normal signals that input is
// closed and are not returned as an error.
func Serve(in io.Reader, out io.Writer, h http.Handler) error {
	inBuf := bufio.NewReader(in)
	outBuf := bufio.NewWriter(out)
	for {
		req, err := http.ReadRequest(inBuf)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		rw := &responseWriter{w: outBuf}
		h.ServeHTTP(rw, req)
	}
}
