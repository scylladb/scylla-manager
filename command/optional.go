// Copyright (C) 2017 ScyllaDB

package command

// OptionalString can tell if the value was set.
type OptionalString struct {
	Value   string
	Changed bool
}

// Set implements flag.Value
func (o *OptionalString) Set(s string) error {
	o.Value = s
	o.Changed = true
	return nil
}

// String implements flag.Value.
func (o *OptionalString) String() string {
	return o.Value
}
