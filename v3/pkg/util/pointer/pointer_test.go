// Copyright (C) 2017 ScyllaDB

package pointer

import (
	"testing"
	"time"

	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
)

func TestInt32Ptr(t *testing.T) {
	val := int32(0)
	ptr := Int32Ptr(val)
	if *ptr != val {
		t.Errorf("expected %d, got %d", val, *ptr)
	}

	val = int32(1)
	ptr = Int32Ptr(val)
	if *ptr != val {
		t.Errorf("expected %d, got %d", val, *ptr)
	}
}

func TestInt32PtrDerefOr(t *testing.T) {
	var val, def int32 = 1, 0

	out := Int32PtrDerefOr(&val, def)
	if out != val {
		t.Errorf("expected %d, got %d", val, out)
	}

	out = Int32PtrDerefOr(nil, def)
	if out != def {
		t.Errorf("expected %d, got %d", def, out)
	}
}

func TestInt64Ptr(t *testing.T) {
	val := int64(0)
	ptr := Int64Ptr(val)
	if *ptr != val {
		t.Errorf("expected %d, got %d", val, *ptr)
	}

	val = int64(1)
	ptr = Int64Ptr(val)
	if *ptr != val {
		t.Errorf("expected %d, got %d", val, *ptr)
	}
}

func TestInt64PtrDerefOr(t *testing.T) {
	var val, def int64 = 1, 0

	out := Int64PtrDerefOr(&val, def)
	if out != val {
		t.Errorf("expected %d, got %d", val, out)
	}

	out = Int64PtrDerefOr(nil, def)
	if out != def {
		t.Errorf("expected %d, got %d", def, out)
	}
}

func TestBoolPtr(t *testing.T) {
	val := false
	ptr := BoolPtr(val)
	if *ptr != val {
		t.Errorf("expected %t, got %t", val, *ptr)
	}

	val = true
	ptr = BoolPtr(true)
	if *ptr != val {
		t.Errorf("expected %t, got %t", val, *ptr)
	}
}

func TestBoolPtrDerefOr(t *testing.T) {
	val, def := true, false

	out := BoolPtrDerefOr(&val, def)
	if out != val {
		t.Errorf("expected %t, got %t", val, out)
	}

	out = BoolPtrDerefOr(nil, def)
	if out != def {
		t.Errorf("expected %t, got %t", def, out)
	}
}

func TestStringPtr(t *testing.T) {
	val := ""
	ptr := StringPtr(val)
	if *ptr != val {
		t.Errorf("expected %s, got %s", val, *ptr)
	}

	val = "a"
	ptr = StringPtr(val)
	if *ptr != val {
		t.Errorf("expected %s, got %s", val, *ptr)
	}
}

func TestStringPtrDerefOr(t *testing.T) {
	val, def := "a", ""

	out := StringPtrDerefOr(&val, def)
	if out != val {
		t.Errorf("expected %s, got %s", val, out)
	}

	out = StringPtrDerefOr(nil, def)
	if out != def {
		t.Errorf("expected %s, got %s", def, out)
	}
}

func TestFloat32Ptr(t *testing.T) {
	val := float32(0)
	ptr := Float32Ptr(val)
	if *ptr != val {
		t.Errorf("expected %f, got %f", val, *ptr)
	}

	val = float32(0.1)
	ptr = Float32Ptr(val)
	if *ptr != val {
		t.Errorf("expected %f, got %f", val, *ptr)
	}
}

func TestFloat32PtrDerefOr(t *testing.T) {
	var val, def float32 = 0.1, 0

	out := Float32PtrDerefOr(&val, def)
	if out != val {
		t.Errorf("expected %f, got %f", val, out)
	}

	out = Float32PtrDerefOr(nil, def)
	if out != def {
		t.Errorf("expected %f, got %f", def, out)
	}
}

func TestFloat64Ptr(t *testing.T) {
	val := float64(0)
	ptr := Float64Ptr(val)
	if *ptr != val {
		t.Errorf("expected %f, got %f", val, *ptr)
	}

	val = float64(0.1)
	ptr = Float64Ptr(val)
	if *ptr != val {
		t.Errorf("expected %f, got %f", val, *ptr)
	}
}

func TestFloat64PtrDerefOr(t *testing.T) {
	var val, def float64 = 0.1, 0

	out := Float64PtrDerefOr(&val, def)
	if out != val {
		t.Errorf("expected %f, got %f", val, out)
	}

	out = Float64PtrDerefOr(nil, def)
	if out != def {
		t.Errorf("expected %f, got %f", def, out)
	}
}

func TestTimePtr(t *testing.T) {
	val := timeutc.Now()
	ptr := TimePtr(val)
	if *ptr != val {
		t.Errorf("expected %v, got %v", val, *ptr)
	}

	val = timeutc.Now()
	ptr = TimePtr(val)
	if *ptr != val {
		t.Errorf("expected %v, got %v", val, *ptr)
	}
}

func TestTimePtrDerefOr(t *testing.T) {
	var val, def time.Time = timeutc.Now(), time.Time{}

	out := TimePtrDerefOr(&val, def)
	if out != val {
		t.Errorf("expected %v, got %v", val, out)
	}

	out = TimePtrDerefOr(nil, def)
	if out != def {
		t.Errorf("expected %v, got %v", def, out)
	}
}
