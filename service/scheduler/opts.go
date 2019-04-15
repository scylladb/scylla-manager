// Copyright (C) 2017 ScyllaDB

package scheduler

import "encoding/json"

// Properties is a collection of key-value pairs describing properties of a task.
type Properties = json.RawMessage

// Opt specifies a custom function that can be used to decorate task properties
// before sending for execution to Runner.
type Opt func(Properties) Properties

// NoContinue sets "continue" to false.
func NoContinue(p Properties) Properties {
	return setValue(p, "continue", false)
}

func setValue(p Properties, key string, value interface{}) Properties {
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
