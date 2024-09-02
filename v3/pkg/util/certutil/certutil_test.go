// Copyright (C) 2017 ScyllaDB

package certutil

import (
	"crypto/tls"
	"net/http"
	"testing"
	"time"
)

func TestGenerateSelfSignedCertificate(t *testing.T) {
	cert, err := GenerateSelfSignedCertificate([]string{"localhost"})
	if err != nil {
		t.Fatalf("generateX509KeyPair() error %s", err)
	}

	s := http.Server{
		Addr: "localhost:0",
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
		},
	}

	time.AfterFunc(250*time.Millisecond, func() {
		s.Close()
	})

	if err := s.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
		t.Fatalf("ListenAndServe() error %s", err)
	}
}
