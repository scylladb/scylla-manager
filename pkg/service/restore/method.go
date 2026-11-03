// Copyright (C) 2026 ScyllaDB

package restore

import (
	"slices"
	"strings"

	"github.com/pkg/errors"
)

// Method describes a single API used by SM for restoring data.
type Method string

const (
	// MethodRclone restores data with the rclone API.
	MethodRclone Method = "rclone"
	// MethodNative restores data with the native scylla restore API.
	MethodNative Method = "native"
	// MethodTablet restores data with the tablet aware restore API.
	MethodTablet Method = "tablet"
)

// MethodList is the raw --method flag value describing
// which methods SM is allowed to use during restore.
// It's either MethodAuto or a comma separated list of Method values.
// When more than one method is allowed, SM chooses the method
// according to methodOrder for every restored batch.
type MethodList string

// MethodAuto allows all supported restore methods.
const MethodAuto MethodList = "auto"

const defaultMethod = MethodList(MethodRclone)

// methodOrder defines the order in which SM prefers restore methods
// when more than one of them is allowed.
var methodOrder = []Method{MethodTablet, MethodNative, MethodRclone}

// supportedMethods lists methods accepted in MethodList.
var supportedMethods = []Method{MethodNative, MethodRclone}

// validateMethod validates Target.Method.
func (t Target) validateMethod() error {
	methods, err := t.Method.parse()
	if err != nil {
		return err
	}
	// Restore schema supports only the default method
	if t.RestoreSchema && !slices.Equal(methods, []Method{MethodRclone}) {
		return errors.New("restore schema does not support '--method' flag")
	}
	return nil
}

// parse returns allowed methods sorted according to methodOrder.
// Empty MethodList allows just the defaultMethod.
func (ml MethodList) parse() ([]Method, error) {
	if ml == "" {
		return defaultMethod.parse()
	}
	if ml == MethodAuto {
		return slices.Clone(supportedMethods), nil
	}

	var out []Method
	for raw := range strings.SplitSeq(string(ml), ",") {
		m := Method(strings.TrimSpace(raw))
		switch {
		case MethodList(m) == MethodAuto:
			return nil, errors.Errorf("method %q can't be combined with other methods", m)
		case !slices.Contains(supportedMethods, m):
			return nil, errors.Errorf("unknown method: %q", m)
		case slices.Contains(out, m):
			return nil, errors.Errorf("method %q is specified multiple times", m)
		}
		out = append(out, m)
	}
	slices.SortFunc(out, func(a, b Method) int {
		return slices.Index(methodOrder, a) - slices.Index(methodOrder, b)
	})
	return out, nil
}

// allows returns whether method is allowed to be used for restore.
// It assumes that MethodList was already validated.
func (ml MethodList) allows(m Method) bool {
	methods, err := ml.parse()
	if err != nil {
		return false
	}
	return slices.Contains(methods, m)
}

// explicit returns whether method was explicitly listed in MethodList
// as opposed to being allowed via MethodAuto. Explicitly listed methods
// have different validation than the same methods allowed via MethodAuto.
func (ml MethodList) explicit(m Method) bool {
	return ml != MethodAuto && ml.allows(m)
}

// addMethod adds method to the set of methods preserving methodOrder.
func addMethod(methods []Method, m Method) []Method {
	if slices.Contains(methods, m) {
		return methods
	}
	methods = append(methods, m)
	slices.SortFunc(methods, func(a, b Method) int {
		return slices.Index(methodOrder, a) - slices.Index(methodOrder, b)
	})
	return methods
}
