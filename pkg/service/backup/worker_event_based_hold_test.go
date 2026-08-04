// Copyright (C) 2026 ScyllaDB

package backup

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"
)

type holdCall struct {
	paths []string
	hold  bool
}

func TestEventBasedHoldHandler(t *testing.T) {
	t.Parallel()

	type remoteObject struct {
		name string
		hold bool
	}
	testCases := []struct {
		name      string
		batchSize int
		local     []string
		remote    []remoteObject
		expected  []holdCall
	}{
		{
			name:      "sets missing holds and removes stale holds",
			batchSize: 10,
			local:     []string{"a", "b"},
			remote: []remoteObject{
				{name: "a", hold: false},
				{name: "b", hold: true},
				{name: "c", hold: true},
				{name: "d", hold: false},
			},
			expected: []holdCall{
				{paths: []string{"a"}, hold: true},
				{paths: []string{"c"}, hold: false},
			},
		},
		{
			name:      "flushes full batches while streaming",
			batchSize: 2,
			local:     []string{"a", "b", "c"},
			remote: []remoteObject{
				{name: "a", hold: false},
				{name: "b", hold: false},
				{name: "x", hold: true},
				{name: "y", hold: true},
				{name: "c", hold: false},
				{name: "z", hold: true},
			},
			expected: []holdCall{
				{paths: []string{"a", "b"}, hold: true},
				{paths: []string{"x", "y"}, hold: false},
				{paths: []string{"c"}, hold: true},
				{paths: []string{"z"}, hold: false},
			},
		},
		{
			name:      "does not call apply for no-op states",
			batchSize: 2,
			local:     []string{"a"},
			remote: []remoteObject{
				{name: "a", hold: true},
				{name: "b", hold: false},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var got []holdCall
			apply := func(_ context.Context, paths []string, hold bool) error {
				got = append(got, holdCall{
					paths: append([]string{}, paths...),
					hold:  hold,
				})
				return nil
			}
			h := newEventBasedHoldHandler(apply, tc.batchSize)
			for _, name := range tc.local {
				h.addLocal(name)
			}
			h.finalizeLocal()
			for _, remote := range tc.remote {
				h.addRemote(t.Context(), remote.name, remote.hold)
			}
			if err := h.finalize(t.Context()); err != nil {
				t.Fatal(err)
			}

			sortByHold(got)
			sortByHold(tc.expected)
			if !reflect.DeepEqual(got, tc.expected) {
				t.Fatalf("got %+v, expected %+v", got, tc.expected)
			}
		})
	}
}

// sortByHold ensures that holdCall elements are grouped by hold state.
// eventBasedHoldHandler applies holds in FIFO order for objects batched
// with the same hold state, but that's not necessarily the case for objects
// with different hold state, as they are batched separately.
func sortByHold(calls []holdCall) {
	slices.SortStableFunc(calls, func(a, b holdCall) int {
		switch {
		case a.hold && !b.hold:
			return -1
		case !a.hold && b.hold:
			return 1
		default:
			return 0
		}
	})
}

// TestEventBasedHoldHandlerApplyErrorDrainsRequests validates that a failing apply
// doesn't stall the handler - all pending requests are still consumed by the worker
// and the first error is returned.
func TestEventBasedHoldHandlerApplyError(t *testing.T) {
	t.Parallel()

	firstErr := errors.New("first apply failed")
	secondErr := errors.New("second apply failed")

	var got []holdCall
	applyErrs := []error{firstErr, secondErr}
	apply := func(_ context.Context, paths []string, hold bool) error {
		got = append(got, holdCall{paths: slices.Clone(paths), hold: hold})
		if len(applyErrs) == 0 {
			return nil
		}
		err := applyErrs[0]
		applyErrs = applyErrs[1:]
		return err
	}

	h := newEventBasedHoldHandler(apply, 1)
	for _, name := range []string{"a", "b", "c"} {
		h.addLocal(name)
	}
	h.finalizeLocal()
	for _, name := range []string{"a", "b", "c"} {
		h.addRemote(t.Context(), name, false)
	}

	err := h.finalize(t.Context())
	if !errors.Is(err, firstErr) {
		t.Fatalf("got %v, expected %v", err, firstErr)
	}
	if errors.Is(err, secondErr) {
		t.Fatalf("got %v, expected only the first apply error", err)
	}

	expected := []holdCall{
		{paths: []string{"a"}, hold: true},
		{paths: []string{"b"}, hold: true},
		{paths: []string{"c"}, hold: true},
	}
	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("got %+v, expected %+v", got, expected)
	}
}

// TestEventBasedHoldHandlerContextCancel validates that a canceled context
// unblocks flush waiting for a busy worker, that the worker skips not yet
// applied requests, and that finalize still cleans the worker goroutine up.
func TestEventBasedHoldHandlerContextCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var (
		mu           sync.Mutex
		got          []holdCall
		busy         = make(chan struct{})
		releaseApply = make(chan struct{})
	)
	apply := func(_ context.Context, paths []string, hold bool) error {
		mu.Lock()
		got = append(got, holdCall{paths: slices.Clone(paths), hold: hold})
		mu.Unlock()
		close(busy)
		<-releaseApply
		return nil
	}

	h := newEventBasedHoldHandler(apply, 1)
	for _, name := range []string{"a", "b", "c"} {
		h.addLocal(name)
	}
	h.finalizeLocal()
	// "a" is taken by the worker, which blocks inside apply.
	h.addRemote(ctx, "a", false)

	select {
	case <-busy:
	case <-time.After(time.Second):
		t.Fatal("worker wasn't busy")
	}

	// "b" fills the request channel buffer.
	h.addRemote(ctx, "b", false)
	// "c" can't be handed over to the busy worker, so it stays in the batch buffer.
	h.addRemote(ctx, "c", false)

	cancel()

	// Flushing "c" can't succeed, as the worker is still busy,
	// so it must return because of the canceled context.
	flushErr := make(chan error, 1)
	go func() { flushErr <- h.flush(ctx, true) }()

	select {
	case err := <-flushErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, expected %v", err, context.Canceled)
		}
	case <-time.After(time.Second):
		t.Fatal("flush blocked")
	}

	close(releaseApply)
	finalizeErr := make(chan error, 1)
	go func() { finalizeErr <- h.finalize(ctx) }()

	select {
	case err := <-finalizeErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v, expected %v", err, context.Canceled)
		}
	case <-time.After(time.Second):
		t.Fatal("finalize blocked")
	}

	// Only "a" made it to apply before the cancellation,
	// "b" and "c" must have been skipped by the worker.
	if expected := []holdCall{{paths: []string{"a"}, hold: true}}; !reflect.DeepEqual(got, expected) {
		t.Fatalf("got %+v, expected %+v", got, expected)
	}
}

func TestEventBasedHoldHandlerAddRemoteDoesNotBlockOnApply(t *testing.T) {
	t.Parallel()

	releaseApply := make(chan struct{})
	applyCalls := make(chan []string, 4)
	apply := func(_ context.Context, paths []string, _ bool) error {
		<-releaseApply
		applyCalls <- append([]string{}, paths...)
		return nil
	}

	h := newEventBasedHoldHandler(apply, 1)
	h.addLocal("a")
	h.addLocal("b")
	h.addLocal("c")
	h.addLocal("d")
	h.finalizeLocal()
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.addRemote(t.Context(), "a", false)
		h.addRemote(t.Context(), "b", false)
		h.addRemote(t.Context(), "c", false)
		h.addRemote(t.Context(), "d", false)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("addRemote blocked")
	}

	close(releaseApply)
	if err := h.finalize(t.Context()); err != nil {
		t.Fatal(err)
	}

	close(applyCalls)
	var got [][]string
	for call := range applyCalls {
		got = append(got, call)
	}
	if expected := [][]string{{"a"}, {"b"}, {"c"}, {"d"}}; !reflect.DeepEqual(got, expected) {
		t.Fatalf("got calls %v, expected %v", got, expected)
	}
}
