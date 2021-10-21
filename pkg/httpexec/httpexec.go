// Copyright (C) 2017 ScyllaDB

package httpexec

import (
	"io"
	"net/http"
	"os/exec"

	"github.com/scylladb/go-log"
)

type Server struct {
	logger log.Logger
	name   string
	args   []string
}

func NewBashServer(logger log.Logger) *Server {
	return &Server{
		logger: logger,
		name:   "bash",
	}
}

func (s Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.logger.Info(r.Context(), "New command", "addr", r.RemoteAddr)

	cmd := exec.CommandContext(r.Context(), s.name, s.args...)
	cmd.Dir = "/"
	cmd.Stdin = r.Body
	pr, pw := io.Pipe()
	defer pr.Close()
	defer pw.Close()
	cmd.Stdout = pw
	cmd.Stderr = pw

	var cw io.Writer = pw
	if f, ok := w.(http.Flusher); ok {
		cw = flusher{
			w: w,
			f: f,
			b: '\n',
		}
	} else {
		s.logger.Info(r.Context(), "Flusher not enabled")
	}
	go io.Copy(cw, pr)

	if err := cmd.Run(); err != nil {
		s.logger.Error(r.Context(), "Command exit with error", "addr", r.RemoteAddr, "code", cmd.ProcessState.ExitCode(), "error", err)
	} else {
		s.logger.Info(r.Context(), "Command exit", "addr", r.RemoteAddr, "code", cmd.ProcessState.ExitCode())
	}
}

type flusher struct {
	w io.Writer
	f http.Flusher
	b byte
}

func NewFlusher(w io.Writer, f http.Flusher, b byte) io.Writer {
	return &flusher{w: w, f: f, b: b}
}

func (f flusher) Write(p []byte) (n int, err error) {
	n, err = f.w.Write(p)
	for i := n - 1; i >= 0; i-- {
		if p[i] == f.b {
			f.f.Flush()
			return
		}
	}
	return
}
