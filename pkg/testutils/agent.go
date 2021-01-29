// Copyright (C) 2017 ScyllaDB

package testutils

import (
	"flag"
)

var flagAgentAuthToken = flag.String("agent-auth-token", "", "token to authenticate with agent")

// AgentAuthToken returns token to authenticate with agent.
func AgentAuthToken() string {
	if !flag.Parsed() {
		flag.Parse()
	}
	return *flagAgentAuthToken
}
