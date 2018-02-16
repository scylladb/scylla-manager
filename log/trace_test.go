// Copyright (C) 2017 ScyllaDB

package log

import (
	"context"
	"testing"
)

func TestWithTraceID(t *testing.T) {
	ctx := WithTraceID(context.Background())
	id := TraceID(ctx)
	if id == "" {
		t.Fatal("missing id")
	}

	ctx = WithTraceID(ctx)
	if id != TraceID(ctx) {
		t.Fatal("expected", id, "got", TraceID(ctx))
	}
}

func TestCopyTraceID(t *testing.T) {
	from := WithTraceID(context.Background())
	ctx := context.Background()

	cp := CopyTraceID(ctx, from)
	if TraceID(from) != TraceID(cp) {
		t.Fatal("id not copied")
	}
}

func TestTraceID(t *testing.T) {
	ctx := context.Background()
	if TraceID(ctx) != "" {
		t.Fatal("expected empty")
	}

	if TraceID(context.Background()) != "" {
		t.Fatal("expected empty")
	}
}

func BenchmarkRandTraceID(b *testing.B) {
	for i := 0; i < b.N; i++ {
		newTraceID()
	}
}
