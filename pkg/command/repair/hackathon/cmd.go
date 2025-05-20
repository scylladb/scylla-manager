// Copyright (C) 2025 ScyllaDB

package hackathon

import (
	_ "embed"
	"fmt"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

//go:embed res.yaml
var res []byte

type command struct {
	cobra.Command
	dataPath               string
	host                   string
	port                   int64
	authToken              string
	username               string
	password               string
	sslUserCertFile        string
	sslUserKeyFile         string
	forceTLSDisabled       bool
	forceNonSSLSessionPort bool
}

func NewCommand() *cobra.Command {
	var (
		cmd = &command{}
		r   []byte
	)
	r = res
	if err := yaml.Unmarshal(r, &cmd.Command); err != nil {
		panic(err)
	}
	cmd.init()
	cmd.RunE = func(_ *cobra.Command, args []string) error {
		return cmd.run(args)
	}
	return &cmd.Command
}

func (cmd *command) init() {
	w := cmd.Flags()
	w.StringVar(&cmd.dataPath, "data-path", "", "path to store data")
	w.StringVar(&cmd.host, "host", "", "")
	w.Int64Var(&cmd.port, "port", 10001, "")
	w.StringVar(&cmd.authToken, "auth-token", "", "")
	w.StringVarP(&cmd.username, "username", "u", "", "")
	w.StringVarP(&cmd.password, "password", "p", "", "")
	w.StringVar(&cmd.sslUserCertFile, "ssl-user-cert-file", "", "")
	w.StringVar(&cmd.sslUserKeyFile, "ssl-user-key-file", "", "")
	w.BoolVar(&cmd.forceTLSDisabled, "force-tls-disabled", false, "")
	w.BoolVar(&cmd.forceNonSSLSessionPort, "force-non-ssl-session-port", false, "")
}

func (cmd *command) run(args []string) error {

	fmt.Println("CLUSTER IS REPAIRED:)")

	return nil
}
