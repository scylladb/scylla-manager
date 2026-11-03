// Copyright (C) 2026 ScyllaDB

package restore

import (
	"slices"

	"github.com/pkg/errors"
)

// Method describes which API should be used by SM during restore.
type Method string

const (
	// MethodAuto means that SM will use native restore API when possible, and rclone API otherwise.
	MethodAuto Method = "auto"
	// MethodRclone means that SM will only use rclone API.
	MethodRclone Method = "rclone"
	// MethodNative means that SM will only use native scylla restore API (whether it's configured or not).
	MethodNative Method = "native"
	// MethodTablet means that SM uses tablet aware restore API.
	MethodTablet Method = "tablet"
)

const defaultMethod = MethodRclone

// methodOrder defines the order in which methods are reported in progress.
var methodOrder = []Method{MethodTablet, MethodNative, MethodRclone}

// validateMethod validates Target.Method.
func (t Target) validateMethod() error {
	if t.RestoreSchema && t.Method != defaultMethod {
		return errors.New("restore schema does not support '--method' flag")
	}
	if !slices.Contains([]Method{MethodAuto, MethodRclone, MethodNative}, t.Method) {
		return errors.New("unknown method: " + string(t.Method))
	}
	return nil
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
