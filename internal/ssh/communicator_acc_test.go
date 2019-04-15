// Copyright (C) 2017 ScyllaDB

// +build acc

package ssh

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
)

func newTestCommunicator(host, user, pass string) *Communicator {
	var (
		config = DefaultConfig().WithPasswordAuth(user, pass)
		dial   = ContextDialer(&net.Dialer{})
		logger = log.NewDevelopment()
	)
	return NewCommunicator(host, config, dial, logger)
}

func newTestCommunicatorFromEnv(t *testing.T) *Communicator {
	t.Helper()

	host := os.Getenv("TEST_SSH_HOST")
	user := os.Getenv("TEST_SSH_USER")
	pass := os.Getenv("TEST_SSH_PASS")

	if host == "" || user == "" || pass == "" {
		t.Skip("Missing environment variables TEST_SSH_HOST, TEST_SSH_USER, TEST_SSH_PASS")
	}

	return newTestCommunicator(host, user, pass)
}

func TestAccStart(t *testing.T) {
	c := newTestCommunicatorFromEnv(t)

	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}

	t.Run("command success", func(t *testing.T) {
		var cmd Cmd
		stdout := new(bytes.Buffer)
		cmd.Command = "echo foo"
		cmd.Stdout = stdout

		ctx := context.Background()
		if err := c.Start(ctx, &cmd); err != nil {
			t.Fatalf("error executing remote command: %s", err)
		}

		if err := cmd.Wait(); err != nil {
			t.Fatal("command failed", err)
		}

		if stdout.String() != "foo\n" {
			t.Fatal("expected", "foo", "got", stdout.String())
		}
	})

	t.Run("command failure", func(t *testing.T) {
		var cmd Cmd
		cmd.Command = "false"

		ctx := context.Background()
		if err := c.Start(ctx, &cmd); err != nil {
			t.Fatalf("error executing remote command: %s", err)
		}

		err := cmd.Wait()
		if err == nil {
			t.Fatal("expected communicator error")
		}
		_, ok := err.(*ExitError)
		if !ok {
			t.Fatal("expected exit error")
		}
	})

	t.Run("context canceled", func(t *testing.T) {
		var cmd Cmd
		cmd.Command = "sleep 5"

		ctx, cancel := context.WithCancel(context.Background())
		if err := c.Start(ctx, &cmd); err != nil {
			t.Fatalf("error executing remote command: %s", err)
		}

		// Cancel the context, to cause the command to fail
		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		if err := cmd.Wait(); errors.Cause(err) != context.Canceled {
			t.Fatal("expected context.Canceled", "got", err)
		}
	})
}

func TestAccLostConnection(t *testing.T) {
	c := newTestCommunicatorFromEnv(t)

	var cmd Cmd
	cmd.Command = "sleep 5"

	ctx := context.Background()
	if err := c.Start(ctx, &cmd); err != nil {
		t.Fatalf("error executing remote command: %s", err)
	}

	// Disconnect the communicator transport, to cause the command to fail
	go func() {
		time.Sleep(100 * time.Millisecond)
		c.Disconnect()
	}()

	if err := cmd.Wait(); err == nil {
		t.Fatal("expected communicator error")
	}
}

func TestAccUploadFile(t *testing.T) {
	c := newTestCommunicatorFromEnv(t)

	const MB = int64(1 << 20)

	r := rand.NewSource(time.Now().Unix())
	TempFile := func() string {
		return fmt.Sprint("/tmp/ssh-upload-test-", r.Int63())
	}

	t.Run("small file", func(t *testing.T) {
		var (
			path    = TempFile()
			perm    = os.FileMode(0700)
			content = []byte("this is the file content")
		)

		ctx := context.Background()
		if err := c.Upload(ctx, path, perm, bytes.NewReader(content)); err != nil {
			t.Fatal("error uploading file", err)
		}

		assertStdout(t, c, "stat -c '%A' "+path, perm.String())
		assertStdout(t, c, "stat -c '%s' "+path, fmt.Sprint(len(content)))
	})

	t.Run("big file", func(t *testing.T) {
		var (
			path = TempFile()
			perm = os.FileMode(0644)
			size = 100 * MB
		)

		ctx := context.Background()
		if err := c.Upload(ctx, path, perm, io.LimitReader(rand.New(r), size)); err != nil {
			t.Fatal("error uploading file", err)
		}

		assertStdout(t, c, "stat -c '%A' "+path, perm.String())
		assertStdout(t, c, "stat -c '%s' "+path, fmt.Sprint(size))
	})

	t.Run("context cancelled", func(t *testing.T) {
		var (
			path = TempFile()
			perm = os.FileMode(0420)
			size = 100 * MB
		)

		ctx, cancel := context.WithCancel(context.Background())
		source := &contextCancellingReader{
			limit:  MB,
			cancel: cancel,
			inner:  rand.New(r),
		}

		if err := c.Upload(ctx, path, perm, io.LimitReader(source, size)); err != context.Canceled {
			t.Fatal("expected context.Canceled", "got", err)
		}

		var cmd Cmd
		cmd.Command = "stat -c '%A' " + path

		if err := c.Start(ctx, &cmd); err != nil {
			t.Fatalf("error executing remote command: %s", err)
		}
		if err := cmd.Wait(); err == nil {
			t.Fatal("expected file does not exist")
		}
	})
}

type contextCancellingReader struct {
	size   int
	limit  int64
	cancel context.CancelFunc
	inner  io.Reader

	read int64
}

func (r *contextCancellingReader) Read(p []byte) (n int, err error) {
	n, err = r.inner.Read(p)

	r.read += int64(n)
	if r.cancel != nil && r.read >= r.limit {
		r.cancel()
		r.cancel = nil
	}
	return
}

func (r *contextCancellingReader) Len() int {
	return r.size
}

func assertStdout(t *testing.T, c *Communicator, command string, expected string) {
	t.Helper()

	var cmd Cmd
	stdout := new(bytes.Buffer)
	cmd.Command = command
	cmd.Stdout = stdout

	ctx := context.Background()
	if err := c.Start(ctx, &cmd); err != nil {
		t.Fatalf("error executing remote command: %s", err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal("command failed", err)
	}
	if stdout.String() != expected+"\n" {
		t.Fatal("expected", "foo", "got", stdout.String())
	}
}
