// Copyright (C) 2017 ScyllaDB

package main

import (
	"bytes"
	"io"
	"io/ioutil"
	"testing"
	"time"
)

func TestListenerAcceptOnce(t *testing.T) {
	w := &bytes.Buffer{}
	r := &bytes.Buffer{}

	l := newListener(w, ioutil.NopCloser(r))
	conn, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	time.AfterFunc(50*time.Millisecond, func() {
		conn.Close()
	})
	_, err = l.Accept()
	if err != io.EOF {
		t.Fatal("expected io.EOF got", err)
	}
}

func TestListenerConn(t *testing.T) {
	const payload = "Foo"

	w := &bytes.Buffer{}
	r := bytes.NewBufferString(payload)

	l := newListener(w, ioutil.NopCloser(r))
	conn, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("read", func(t *testing.T) {
		b := make([]byte, 5)
		n, err := conn.Read(b)
		if err != nil {
			t.Fatal(err)
		}
		if n != len(payload) {
			t.Fatal("expected", len(payload), "got", n)
		}
		if string(b[:n]) != payload {
			t.Fatal("expected", payload, "got", string(b))
		}
	})

	t.Run("write", func(t *testing.T) {
		b := []byte(payload)
		n, err := conn.Write(b)
		if err != nil {
			t.Fatal(err)
		}
		if n != len(payload) {
			t.Fatal("expected", len(payload), "got", n)
		}
		if w.String() != payload {
			t.Fatal("expected", payload, "got", w.String())
		}
	})
}
