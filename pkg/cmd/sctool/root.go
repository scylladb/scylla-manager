// Copyright (C) 2017 ScyllaDB

package main

import (
	"crypto/tls"
	"net/http"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg"
	"github.com/scylladb/scylla-manager/pkg/command/flag"
	"github.com/scylladb/scylla-manager/pkg/managerclient"
	"github.com/spf13/cobra"
)

type rootCommand struct {
	cobra.Command
	client *managerclient.Client

	apiURL      string
	apiCertFile string
	apiKeyFile  string
}

func newRootCommand(client *managerclient.Client) *cobra.Command {
	cmd := &rootCommand{
		Command: cobra.Command{
			Use:   "sctool",
			Short: "Scylla Manager " + pkg.Version(),
			Long:  "Scylla Manager " + pkg.Version() + ".\n\nDocumentation is available online at https://manager.docs.scylladb.com/.",
		},
		client: client,
	}
	cmd.init()
	cmd.PersistentPreRunE = func(_ *cobra.Command, args []string) error {
		return cmd.preRun()
	}
	return &cmd.Command
}

func (cmd *rootCommand) init() {
	w := flag.Wrap(cmd.PersistentFlags())
	w.GlobalAPIURL(&cmd.apiURL)
	w.GlobalAPICertFile(&cmd.apiCertFile)
	w.GlobalAPIKeyFile(&cmd.apiKeyFile)
}

func (cmd *rootCommand) preRun() error {
	if cmd.IsAdditionalHelpTopicCommand() {
		return nil
	}

	if cmd.apiCertFile != "" && cmd.apiKeyFile == "" {
		return errors.New("missing --api-key-file flag")
	}
	if cmd.apiKeyFile != "" && cmd.apiCertFile == "" {
		return errors.New("missing --api-cert-file flag")
	}

	tlsConfig := managerclient.DefaultTLSConfig()

	if cmd.apiCertFile != "" {
		cert, err := tls.LoadX509KeyPair(cmd.apiCertFile, cmd.apiKeyFile)
		if err != nil {
			return errors.Wrap(err, "load client certificate")
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	c, err := managerclient.NewClient(cmd.apiURL, &http.Transport{TLSClientConfig: tlsConfig})
	if err != nil {
		return err
	}

	*cmd.client = c
	return nil
}
