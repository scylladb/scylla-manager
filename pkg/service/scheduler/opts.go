// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"context"
	"encoding/json"

	"github.com/pkg/errors"
)

// Properties is a collection of key-value pairs describing properties of a task.
type Properties json.RawMessage

// MarshalJSON returns p as the JSON encoding of p.
func (p Properties) MarshalJSON() ([]byte, error) {
	if p == nil {
		return []byte("null"), nil
	}
	return p, nil
}

// UnmarshalJSON sets *p to a copy of data.
func (p *Properties) UnmarshalJSON(data []byte) error {
	if p == nil {
		return errors.New("json.RawMessage: UnmarshalJSON on nil pointer")
	}
	*p = append((*p)[0:0], data...)
	return nil
}

func (p Properties) String() string {
	return string(p)
}

// Set returns a copy of Properties where key=value.
func (p Properties) Set(key string, value interface{}) Properties {
	m := map[string]interface{}{}
	if err := json.Unmarshal(p, &m); err != nil {
		panic(err)
	}
	m[key] = value
	v, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	return v
}

// AsJSON casts Properties to json.RawMessage.
func (p Properties) AsJSON() json.RawMessage {
	return json.RawMessage(p)
}

// Opt specifies a custom function that can be used to decorate task properties
// before sending for execution to Runner.
type Opt func(Properties) Properties

// NoContinue sets "continue" to false.
func NoContinue(p Properties) Properties {
	return p.Set("continue", false)
}

// TaskOptFunc is a global Opt variant.
type TaskOptFunc func(ctx context.Context, task Task) (Properties, error)
