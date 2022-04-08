// Copyright (C) 2017 ScyllaDB

package scyllaclient_test

import (
	"bytes"
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient/scyllaclienttest"
	"github.com/scylladb/scylla-manager/v3/pkg/util/pointer"
)

func fastRetry(config *scyllaclient.Config) {
	config.Backoff.MaxRetries = 3
	config.Backoff.WaitMin = 50 * time.Millisecond
}

func TestRetryRest(t *testing.T) {
	s := repairTestSuite{
		invokeClient: func(ctx context.Context, host string, client *scyllaclient.Client) error {
			return client.RclonePut(ctx, host, "s3://foo/bar", bytes.NewBufferString("bar"))
		},
	}
	s.Run(t)
}

func TestRetrySwagger(t *testing.T) {
	s := repairTestSuite{
		invokeClient: func(ctx context.Context, host string, client *scyllaclient.Client) error {
			_, err := client.NodeInfo(ctx, host)
			return err
		},
	}
	s.Run(t)
}

type repairTestSuite struct {
	invokeClient func(context.Context, string, *scyllaclient.Client) error
}

func (s repairTestSuite) Run(t *testing.T) {
	t.Run("TestRetrySingleHost", s.TestRetrySingleHost)
	t.Run("TestNoRetry", s.TestNoRetry)
	t.Run("TestRetryCancelContext", s.TestRetryCancelContext)
	t.Run("TestRetryShouldRetryHandler", s.TestRetryShouldRetryHandler)
	t.Run("TestRetryTimeout", s.TestRetryTimeout)
}

func (s repairTestSuite) TestRetrySingleHost(t *testing.T) {
	t.Parallel()

	t.Run("error", func(t *testing.T) {
		host, port, closeServer := scyllaclienttest.MakeServer(t, scyllaclienttest.RespondStatus(t, 999, 999, 999, 999, 200))
		defer closeServer()
		client := scyllaclienttest.MakeClient(t, host, port, fastRetry)

		err := s.invokeClient(context.Background(), host, client)
		if err == nil {
			t.Fatalf("invokeClient() expected error")
		}
		if !strings.Contains(err.Error(), "giving up after 4 attempts") {
			t.Fatalf("invokeClient() error = %s, expected giving up after 4 attempts", err.Error())
		}
	})

	t.Run("success", func(t *testing.T) {
		host, port, closeServer := scyllaclienttest.MakeServer(t, scyllaclienttest.RespondStatus(t, 999, 999, 999, 200))
		defer closeServer()
		client := scyllaclienttest.MakeClient(t, host, port, fastRetry)

		err := s.invokeClient(context.Background(), host, client)
		if err != nil {
			t.Fatal("invokeClient() error", err)
		}
	})
}

func (s repairTestSuite) TestNoRetry(t *testing.T) {
	t.Parallel()

	host, port, closeServer := scyllaclienttest.MakeServer(t, scyllaclienttest.RespondStatus(t, 999, 200))
	defer closeServer()
	client := scyllaclienttest.MakeClient(t, host, port, fastRetry)

	ctx := scyllaclient.NoRetry(context.Background())
	err := s.invokeClient(ctx, host, client)
	if err == nil {
		t.Fatalf("invokeClient() expected error")
	}
}

func (s repairTestSuite) TestRetryCancelContext(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name    string
		Handler http.Handler
	}{
		{
			Name:    "Repeat",
			Handler: scyllaclienttest.RespondStatus(t, 999, 999, 999, 200),
		},
		{
			Name:    "Wait",
			Handler: http.HandlerFunc(func(http.ResponseWriter, *http.Request) { time.Sleep(time.Second) }),
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			time.AfterFunc(50*time.Millisecond, cancel)

			host, port, closeServer := scyllaclienttest.MakeServer(t, test.Handler)
			defer closeServer()
			client := scyllaclienttest.MakeClient(t, host, port, fastRetry)

			err := s.invokeClient(ctx, host, client)
			t.Log("invokeClient() error", err)

			if err == nil {
				t.Fatalf("invokeClient() expected error")
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("invokeClient() error=%s, expected context.Canceled", err)
			}
		})
	}
}

func (s repairTestSuite) TestRetryShouldRetryHandler(t *testing.T) {
	t.Parallel()

	table := []struct {
		Name               string
		Handler            http.Handler
		ShouldRetryHandler func(err error) *bool
		Error              string
	}{
		{
			Name:    "Always retry",
			Handler: scyllaclienttest.RespondStatus(t, 400, 400, 400, 400),
			ShouldRetryHandler: func(err error) *bool {
				return pointer.BoolPtr(true)
			},
			Error: "giving up after 4 attempts: agent [HTTP 400]",
		},
		{
			Name:    "Never retry",
			Handler: scyllaclienttest.RespondStatus(t, 999, 999, 999, 999),
			ShouldRetryHandler: func(err error) *bool {
				return pointer.BoolPtr(false)
			},
			Error: "agent [HTTP 999]",
		},
		{
			Name:    "Fallback",
			Handler: scyllaclienttest.RespondStatus(t, 999, 400, 400, 400, 400),
			ShouldRetryHandler: func(err error) *bool {
				return nil
			},
			Error: "giving up after 2 attempts: agent [HTTP 400]",
		},
	}

	for i := range table {
		test := table[i]

		t.Run(test.Name, func(t *testing.T) {
			host, port, closeServer := scyllaclienttest.MakeServer(t, test.Handler)
			defer closeServer()
			client := scyllaclienttest.MakeClient(t, host, port, fastRetry)

			ctx := scyllaclient.WithShouldRetryHandler(context.Background(), test.ShouldRetryHandler)
			err := s.invokeClient(ctx, host, client)
			t.Log("invokeClient() error", err)

			if err == nil || test.Error == "" {
				t.Error("Expected error")
			}
			if !strings.Contains(err.Error(), test.Error) {
				t.Errorf("Wrong error %s, expected %s", err, test.Error)
			}
		})
	}
}

func (s repairTestSuite) TestRetryTimeout(t *testing.T) {
	t.Parallel()

	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		time.Sleep(200 * time.Millisecond)
	})

	shortTimeout := func(config *scyllaclient.Config) {
		config.Timeout = 30 * time.Millisecond
	}

	host, port, closeServer := scyllaclienttest.MakeServer(t, handler)
	defer closeServer()
	client := scyllaclienttest.MakeClient(t, host, port, fastRetry, shortTimeout)

	t.Run("simple", func(t *testing.T) {
		err := s.invokeClient(context.Background(), host, client)
		if err != nil {
			t.Fatal("invokeClient() error", err)
		}
	})
	t.Run("interactive", func(t *testing.T) {
		err := s.invokeClient(scyllaclient.Interactive(context.Background()), host, client)
		if err == nil {
			t.Fatal("invokeClient() expected error")
		}
	})
	t.Run("custom timeout", func(t *testing.T) {
		err := s.invokeClient(scyllaclient.CustomTimeout(context.Background(), 5*time.Millisecond), host, client)
		if err != nil {
			t.Fatal("invokeClient() error", err)
		}
	})
}

func TestRetryHostPool(t *testing.T) {
	t.Parallel()

	t.Run("error", func(t *testing.T) {
		statusCode := map[string]int{
			"127.0.0.1": 999,
			"127.0.0.2": 999,
			"127.0.0.3": 999,
			"127.0.0.4": 999,
			"127.0.0.5": 999,
		}

		_, port, closeServer := scyllaclienttest.MakeServer(t,
			scyllaclienttest.RespondHostStatus(t, statusCode),
			scyllaclienttest.ServerListenOnAddr(t, ":0"),
		)
		defer closeServer()

		multiHost := func(config *scyllaclient.Config) {
			config.Hosts = []string{
				"127.0.0.1",
				"127.0.0.2",
				"127.0.0.3",
				"127.0.0.4",
				"127.0.0.5",
			}
		}
		client := scyllaclienttest.MakeClient(t, "", port, multiHost)

		_, err := client.ClusterName(context.Background())
		if err == nil {
			t.Fatalf("ClusterName() expected error")
		}
		if !strings.Contains(err.Error(), "giving up after 5 attempts") {
			t.Fatalf("ClusterName() error = %s, expected giving up after 5 attempts", err.Error())
		}
	})

	t.Run("success", func(t *testing.T) {
		statusCode := map[string]int{
			"127.0.0.1": 999,
			"127.0.0.2": 200,
		}

		_, port, closeServer := scyllaclienttest.MakeServer(t,
			scyllaclienttest.RespondHostStatus(t, statusCode),
			scyllaclienttest.ServerListenOnAddr(t, ":0"),
		)
		defer closeServer()

		multiHost := func(config *scyllaclient.Config) {
			config.Hosts = []string{
				"127.0.0.1",
				"127.0.0.2",
			}
		}
		client := scyllaclienttest.MakeClient(t, "", port, multiHost)

		_, err := client.ClusterName(context.Background())
		if err != nil {
			t.Fatal("ClusterName() error", err)
		}
	})
}
