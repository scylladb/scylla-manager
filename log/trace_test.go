package log

import (
	"context"
	"testing"
)

func TestWithTraceIDIdempotency(t *testing.T) {
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

func TestTraceID(t *testing.T) {
	if TraceID(nil) != "" {
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
