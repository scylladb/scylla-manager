// Copyright (C) 2017 ScyllaDB

package scyllaclient_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/config/server"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient/scyllaclienttest"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"go.uber.org/goleak"
)

var errMock = errors.New("mock")

type mockProvider struct {
	client *scyllaclient.Client
	err    error
	called bool
}

func (m *mockProvider) Client(ctx context.Context, clusterID uuid.UUID) (*scyllaclient.Client, error) {
	m.called = true
	return m.client, m.err
}

// fakeClientProvider is a ProviderFunc creating clients backed by fake Scylla
// servers. It keeps track of created servers, so that they can be closed at
// the end of the test, and it allows to fail or to block client creation.
type fakeClientProvider struct {
	t *testing.T

	mu           sync.Mutex
	closeServers []func()
	calls        int
	err          error
	block        chan struct{}

	// started is closed when the first blocked client creation begins.
	started chan struct{}
}

func newFakeClientProvider(t *testing.T) *fakeClientProvider {
	t.Helper()
	return &fakeClientProvider{
		t:       t,
		started: make(chan struct{}),
	}
}

// Client is the ProviderFunc.
func (f *fakeClientProvider) Client(_ context.Context, _ uuid.UUID) (*scyllaclient.Client, error) {
	f.mu.Lock()
	f.calls++
	first := f.calls == 1
	err := f.err
	block := f.block
	f.mu.Unlock()

	if err != nil {
		return nil, err
	}
	if first && block != nil {
		close(f.started)
		<-block
	}

	client, closeServer := scyllaclienttest.NewFakeScyllaServer(f.t, "testdata/scylla_api/host_id_map_localhost.json")
	f.mu.Lock()
	f.closeServers = append(f.closeServers, closeServer)
	f.mu.Unlock()
	return client, nil
}

// BlockFirstCall makes the first client creation wait until the returned
// function is called. It must be called before the first creation starts.
func (f *fakeClientProvider) BlockFirstCall() (release func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.block = make(chan struct{})
	block := f.block
	return func() { close(block) }
}

// AwaitFirstCall waits until the first blocked client creation begins.
func (f *fakeClientProvider) AwaitFirstCall() {
	<-f.started
}

// Calls returns the amount of performed client creations.
func (f *fakeClientProvider) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// SetErr makes all following client creations fail with err.
func (f *fakeClientProvider) SetErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

// Close closes all created fake Scylla servers.
func (f *fakeClientProvider) Close() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, closeServer := range f.closeServers {
		closeServer()
	}
	f.closeServers = nil
}

func newCachedProvider(t *testing.T, f scyllaclient.ProviderFunc, validity, hostsValidity, reapInterval time.Duration) *scyllaclient.CachedProvider {
	t.Helper()
	p, err := scyllaclient.NewCachedProvider(f, validity, hostsValidity, reapInterval, log.Logger{})
	if err != nil {
		t.Fatalf("NewCachedProvider() error: %s", err)
	}
	return p
}

func checkCachedClients(t *testing.T, p *scyllaclient.CachedProvider, expected int) {
	t.Helper()
	if got := p.CachedClients(); got != expected {
		t.Fatalf("CachedClients() = %d, expected %d", got, expected)
	}
}

func TestCachedProvider(t *testing.T) {
	t.Parallel()

	id := uuid.MustRandom()
	m := mockProvider{}
	// Short hosts validity makes checking for changed hosts quick to test
	const hostsValidity = 100 * time.Millisecond
	p := newCachedProvider(t, m.Client, server.DefaultConfig().ClientCacheTimeout, hostsValidity, scyllaclient.DefaultReapInterval)
	defer p.Close()

	// Error
	m.err = errMock

	c, err := p.Client(t.Context(), id)
	if !m.called {
		t.Fatal("not called")
	}
	if c != m.client {
		t.Fatal("wrong client")
	}
	if !errors.Is(err, m.err) {
		t.Fatal(err)
	}

	// Success
	client, closeServer := scyllaclienttest.NewFakeScyllaServer(t, "testdata/scylla_api/host_id_map_localhost.json")
	defer closeServer()

	m.client = client
	m.err = nil
	m.called = false

	c, err = p.Client(t.Context(), id)
	if !m.called {
		t.Fatal("not called")
	}
	if c != m.client {
		t.Fatal("wrong client")
	}
	if !errors.Is(err, m.err) {
		t.Fatal(err)
	}

	// Cached
	m.called = false

	c, err = p.Client(t.Context(), id)
	if m.called {
		t.Fatal("called")
	}
	if c != m.client {
		t.Fatal("wrong client")
	}
	if !errors.Is(err, m.err) {
		t.Fatal(err)
	}

	// Cached but changed
	m.called = false
	m.client.Config().Hosts[0] = "" // make hosts change without starting new server
	time.Sleep(2 * hostsValidity)   // cache checks for changed hosts every hostsValidity
	c, err = p.Client(t.Context(), id)
	if !m.called {
		t.Fatal("not called")
	}
	if c != m.client {
		t.Fatal("wrong client")
	}
	if !errors.Is(err, m.err) {
		t.Fatal(err)
	}

	// Invalidate keeps the cache entry, but forces client recreation
	p.Invalidate(id)
	checkCachedClients(t, p, 1)

	m.called = false

	c, err = p.Client(t.Context(), id)
	if !m.called {
		t.Fatal("not called")
	}
	if c != m.client {
		t.Fatal("wrong client")
	}
	if !errors.Is(err, m.err) {
		t.Fatal(err)
	}

	// Delete drops the cache entry, so that the next call recreates it
	p.Delete(id)
	checkCachedClients(t, p, 0)

	m.called = false

	c, err = p.Client(t.Context(), id)
	if !m.called {
		t.Fatal("not called")
	}
	if c != m.client {
		t.Fatal("wrong client")
	}
	if !errors.Is(err, m.err) {
		t.Fatal(err)
	}
	checkCachedClients(t, p, 1)
}

// TestCachedProviderClosesClients ensures that clients dropped from the cache
// are close, and that the cache doesn't keep entries of deleted clusters.
func TestCachedProviderClosesClients(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	f := newFakeClientProvider(t)
	defer f.Close()

	ctx := t.Context()
	// Zero validity ensures that cached clients are always expired,
	// so that every call replaces the cached client.
	p := newCachedProvider(t, f.Client, 0, scyllaclient.DefaultHostsValidity, time.Hour)

	// Replaced clients are closed
	id := uuid.MustRandom()
	for range 3 {
		if _, err := p.Client(ctx, id); err != nil {
			t.Fatalf("Client() error: %s", err)
		}
	}
	checkCachedClients(t, p, 1)

	// Invalidated client is kept in the cache and closed when it's replaced
	p.Invalidate(id)
	checkCachedClients(t, p, 1)
	if _, err := p.Client(ctx, id); err != nil {
		t.Fatalf("Client() error: %s", err)
	}
	checkCachedClients(t, p, 1)

	// Deleted client is closed and dropped from the cache
	p.Delete(id)
	checkCachedClients(t, p, 0)

	// Failed client creation leaves an empty cache entry
	f.SetErr(errMock)
	if _, err := p.Client(ctx, uuid.MustRandom()); !errors.Is(err, errMock) {
		t.Fatalf("Client() error = %s, expected %s", err, errMock)
	}
	checkCachedClients(t, p, 1)
	f.SetErr(nil)

	// Clients left in the cache are closed on Close
	if _, err := p.Client(ctx, uuid.MustRandom()); err != nil {
		t.Fatalf("Client() error: %s", err)
	}
	checkCachedClients(t, p, 2)

	if err := p.Close(); err != nil {
		t.Fatalf("Close() error: %s", err)
	}
	checkCachedClients(t, p, 0)
}

// TestCachedProviderDeleteDuringClientCreation ensures that a client created
// for a cluster deleted in the meantime is closed instead of being cached.
func TestCachedProviderDeleteDuringClientCreation(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	f := newFakeClientProvider(t)
	defer f.Close()
	release := f.BlockFirstCall()

	p := newCachedProvider(t, f.Client, time.Hour, scyllaclient.DefaultHostsValidity, time.Hour)
	ctx := t.Context()
	id := uuid.MustRandom()

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := p.Client(ctx, id); err != nil {
			t.Error("Client() error", err)
		}
	}()

	f.AwaitFirstCall()
	p.Delete(id)
	release()
	<-done

	checkCachedClients(t, p, 0)
	if err := p.Close(); err != nil {
		t.Fatalf("Close() error: %s", err)
	}
}

// TestCachedProviderDeleteAndReAddDuringClientCreation ensures that a client
// created for a cluster deleted in the meantime is closed even when another
// call already put a new entry under the same cluster ID.
func TestCachedProviderDeleteAndReAddDuringClientCreation(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	f := newFakeClientProvider(t)
	defer f.Close()
	p := newCachedProvider(t, f.Client, time.Hour, scyllaclient.DefaultHostsValidity, time.Hour)
	ctx := t.Context()
	id := uuid.MustRandom()

	// Hang Client on new client creation
	release := f.BlockFirstCall()
	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := p.Client(ctx, id); err != nil {
			t.Error("Client() error", err)
		}
	}()

	// Delete client entry while client is being created
	f.AwaitFirstCall()
	p.Delete(id)

	// Make unblocked call creating client with mocked error
	f.SetErr(errMock)
	if _, err := p.Client(ctx, id); !errors.Is(err, errMock) {
		t.Fatalf("Client() error = %s, expected %s", err, errMock)
	}
	f.SetErr(nil)
	checkCachedClients(t, p, 1)

	// Unblock the initial call allowing it to create client
	release()
	<-done

	checkCachedClients(t, p, 1)
	if err := p.Close(); err != nil {
		t.Fatalf("Close() error: %s", err)
	}
	checkCachedClients(t, p, 0)
}

// TestCachedProviderReap ensures that only expired and invalidated clients
// are removed from the cache and closed when reaping.
func TestCachedProviderReap(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	f := newFakeClientProvider(t)
	defer f.Close()

	const validity = time.Hour

	ctx := t.Context()
	// Long reap interval ensures that reaping happens only on explicit Reap
	p := newCachedProvider(t, f.Client, validity, scyllaclient.DefaultHostsValidity, time.Hour)
	valid, invalidated := uuid.MustRandom(), uuid.MustRandom()
	for _, id := range []uuid.UUID{valid, invalidated} {
		if _, err := p.Client(ctx, id); err != nil {
			t.Fatalf("Client() error: %s", err)
		}
	}

	// Valid clients are kept
	p.Reap()
	checkCachedClients(t, p, 2)

	// Invalidated client is removed
	p.Invalidate(invalidated)
	p.Reap()
	checkCachedClients(t, p, 1)

	// Empty entry left by failed client creation is removed
	f.SetErr(errMock)
	if _, err := p.Client(ctx, uuid.MustRandom()); !errors.Is(err, errMock) {
		t.Fatalf("Client() error = %s, expected %s", err, errMock)
	}
	f.SetErr(nil)
	checkCachedClients(t, p, 2)
	p.Reap()
	checkCachedClients(t, p, 1)

	// Client expired for less than validity is kept,
	// so that it can be recreated by the next Client call
	expired := uuid.MustRandom()
	if _, err := p.Client(ctx, expired); err != nil {
		t.Fatalf("Client() error: %s", err)
	}
	p.SetTTL(expired, time.Now().Add(-validity/2))
	p.Reap()
	checkCachedClients(t, p, 2)

	// Client expired for more than validity is removed
	p.SetTTL(expired, time.Now().Add(-2*validity))
	p.Reap()
	checkCachedClients(t, p, 1)

	// Valid client used for the whole test is still cached
	calls := f.Calls()
	if _, err := p.Client(ctx, valid); err != nil {
		t.Fatalf("Client() error: %s", err)
	}
	if f.Calls() != calls {
		t.Fatal("valid client was recreated")
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close() error: %s", err)
	}
}

// TestCachedProviderReapDuringClientCreation ensures that reaper skips
// cache entries of clients which are being created, so that the freshly
// created client is neither closed nor dropped from the cache.
func TestCachedProviderReapDuringClientCreation(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	f := newFakeClientProvider(t)
	defer f.Close()

	p := newCachedProvider(t, f.Client, time.Hour, scyllaclient.DefaultHostsValidity, time.Hour)
	release := f.BlockFirstCall()

	ctx := t.Context()
	id := uuid.MustRandom()

	done := make(chan struct{})
	go func() {
		defer close(done)
		if _, err := p.Client(ctx, id); err != nil {
			t.Error("Client() error", err)
		}
	}()

	f.AwaitFirstCall()
	p.Reap()
	release()
	<-done

	checkCachedClients(t, p, 1)
	// Client created during reap is still cached and valid
	if _, err := p.Client(ctx, id); err != nil {
		t.Fatalf("Client() error: %s", err)
	}
	if calls := f.Calls(); calls != 1 {
		t.Fatalf("Calls() = %d, expected 1", calls)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close() error: %s", err)
	}
}

// TestCachedProviderReaperLoop ensures that reaper periodically removes
// expired clients from the cache and that it's stopped on Close.
func TestCachedProviderReaperLoop(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	f := newFakeClientProvider(t)
	defer f.Close()

	// Zero validity ensures that cached clients are always expired
	p := newCachedProvider(t, f.Client, 0, scyllaclient.DefaultHostsValidity, time.Millisecond)
	for range 3 {
		if _, err := p.Client(t.Context(), uuid.MustRandom()); err != nil {
			t.Fatalf("Client() error: %s", err)
		}
	}

	deadline := time.Now().Add(5 * time.Second)
	for p.CachedClients() > 0 {
		if time.Now().After(deadline) {
			t.Fatalf("CachedClients() = %d, expected reaper to remove all of them", p.CachedClients())
		}
		time.Sleep(time.Millisecond)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close() error: %s", err)
	}
}

// TestCachedProviderClosed ensures that closed provider doesn't serve
// nor cache any clients.
func TestCachedProviderClosed(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	f := newFakeClientProvider(t)
	defer f.Close()

	p := newCachedProvider(t, f.Client, time.Hour, scyllaclient.DefaultHostsValidity, time.Hour)
	ctx := t.Context()
	id := uuid.MustRandom()

	if _, err := p.Client(ctx, id); err != nil {
		t.Fatalf("Client() error: %s", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close() error: %s", err)
	}

	// All calls on closed provider are a no-op
	if _, err := p.Client(ctx, id); err == nil {
		t.Fatal("Client() expected error")
	}
	p.Invalidate(id)
	p.Delete(id)
	if err := p.Close(); err != nil {
		t.Fatalf("Close() error: %s", err)
	}
	checkCachedClients(t, p, 0)
}

// TestCachedProviderParallelCalls calls all CachedProvider methods in parallel
// in order to check that it doesn't deadlock nor leak any resources.
func TestCachedProviderParallelCalls(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	const (
		workers    = 8
		iterations = 50
		// Sleeping between iterations ensures that the test
		// spans multiple client validity periods.
		sleep        = 200 * time.Microsecond
		validity     = time.Millisecond
		reapInterval = time.Millisecond
	)

	f := newFakeClientProvider(t)
	defer f.Close()

	// Small positive validity results in a mix of cache hits and client
	// recreations caused by the expired TTL, Invalidate, Delete and reaper.
	p := newCachedProvider(t, f.Client, validity, scyllaclient.DefaultHostsValidity, reapInterval)
	ctx := t.Context()
	ids := []uuid.UUID{uuid.MustRandom(), uuid.MustRandom(), uuid.MustRandom()}

	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := range iterations {
				time.Sleep(sleep)
				id := ids[i%len(ids)]
				switch w % 4 {
				case 0, 1: // half of the workers call Client
					if _, err := p.Client(ctx, id); err != nil {
						t.Error("Client() error", err)
						return
					}
				case 2: // quarter of the workers call Invalidate
					p.Invalidate(id)
				case 3: // quarter of the workers call Delete
					p.Delete(id)
				}
			}
		})
	}
	wg.Wait()

	// Both the cache hit and the client creation routes should be exercised.
	clientCalls := workers / 2 * iterations
	t.Logf("Client calls: %d, created clients: %d", clientCalls, f.Calls())

	// Deleting all used cluster IDs empties the cache
	for _, id := range ids {
		p.Delete(id)
	}
	checkCachedClients(t, p, 0)

	if err := p.Close(); err != nil {
		t.Fatalf("Close() error: %s", err)
	}
}

func TestNewCachedProviderValidation(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		validity      time.Duration
		hostsValidity time.Duration
		reapInterval  time.Duration
		err           bool
	}{
		{
			name:          "valid",
			validity:      time.Minute,
			hostsValidity: time.Second,
			reapInterval:  time.Minute,
		},
		{
			name:          "zero validity disables caching",
			validity:      0,
			hostsValidity: time.Second,
			reapInterval:  time.Minute,
		},
		{
			name:          "negative validity",
			validity:      -time.Minute,
			hostsValidity: time.Second,
			reapInterval:  time.Minute,
			err:           true,
		},
		{
			name:          "zero hosts validity checks hosts on every call",
			validity:      time.Minute,
			hostsValidity: 0,
			reapInterval:  time.Minute,
		},
		{
			name:          "negative hosts validity",
			validity:      time.Minute,
			hostsValidity: -time.Second,
			reapInterval:  time.Minute,
			err:           true,
		},
		{
			name:          "zero reap interval",
			validity:      time.Minute,
			hostsValidity: time.Second,
			reapInterval:  0,
			err:           true,
		},
		{
			name:          "negative reap interval",
			validity:      time.Minute,
			hostsValidity: time.Second,
			reapInterval:  -time.Minute,
			err:           true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			p, err := scyllaclient.NewCachedProvider(new(mockProvider).Client, tc.validity, tc.hostsValidity, tc.reapInterval, log.Logger{})
			if tc.err {
				if err == nil {
					t.Fatal("NewCachedProvider() expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("NewCachedProvider() error: %s", err)
			}
			if err := p.Close(); err != nil {
				t.Fatalf("Close() error: %s", err)
			}
		})
	}
}
