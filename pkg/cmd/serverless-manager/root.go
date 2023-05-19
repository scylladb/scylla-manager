package main

import (
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

var (
	cfgFile     string
	dbFile      string
	authToken   string
	sslCertFile string
	sslKeyFile  string
	cqlUsername string
	cqlPassword string
	host        string

	rootCmd = &cobra.Command{
		Use:   "backupsls-manager",
		Short: "Scylla Manager for backupsls",

		RunE: func(cmd *cobra.Command, args []string) error {
			return nil
		},
	}
)

func init() {
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config-file", "", "manager config file (default is $PWD/.manager-backupsls.yaml)")
	rootCmd.PersistentFlags().StringVar(&dbFile, "db-file", ":memory:", "SQLite db file (default is :memory:)")
	rootCmd.PersistentFlags().StringVar(&authToken, "auth-token", "", "Scylla-manager-agent authentication token")
	rootCmd.PersistentFlags().StringVar(&sslCertFile, "ssl-cert-file", "", "path to scylla cluster's SSL certificate file")
	rootCmd.PersistentFlags().StringVar(&sslKeyFile, "ssl-key-file", "", "path to scylla cluster's SSL key file")
	rootCmd.PersistentFlags().StringVar(&cqlUsername, "cql-username", "", "CQL username of the cluster")
	rootCmd.PersistentFlags().StringVar(&cqlPassword, "cql-password", "", "CQL password of the cluster")
	rootCmd.PersistentFlags().StringVar(&host, "host", "", "CQL password of the cluster")
}

func initConfig() {
	if cfgFile == "" {
		currDir, err := os.Getwd()
		cobra.CheckErr(err)

		cfgFile = filepath.Join(currDir, ".manager-backupsls.yaml")
	}
}
