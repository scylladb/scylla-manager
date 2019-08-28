// Copyright (C) 2017 ScyllaDB

package mermaidtest

import (
	"flag"
)

var (
	flagAgentAuthToken = flag.String("agent-auth-token", "", "token to authenticate with agent")
)

// AgentAuthToken returns token to authenticate with agent.
func AgentAuthToken() string {
	return *flagAgentAuthToken
}
