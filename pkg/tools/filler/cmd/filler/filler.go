// Copyright (C) 2017 ScyllaDB

package main

import (
	"context"
	"flag"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/pkg/tools/filler"
)

var (
	flagCluster  = flag.String("cluster", "127.0.0.1", "a comma-separated list of host:port tuples of scylla manager db hosts")
	flagKeyspace = flag.String("keyspace", "", "")
	flagUser     = flag.String("user", "", "")
	flagPassword = flag.String("password", "", "")
	flagSize     = flag.Int64("size", 0, "")
	flagTables   = flag.Int("tables", 0, "")

	flagPages    = flag.Int("pages", 10, "")
	flagParallel = flag.Int("parallel", runtime.NumCPU(), "")
)

func init() {
	rand.Seed(666)
}

func main() {
	flag.Parse()

	c := gocql.NewCluster(strings.Split(*flagCluster, ",")...)
	c.Consistency = gocql.One
	c.Timeout = 5 * time.Second
	c.Keyspace = *flagKeyspace
	if *flagUser != "" {
		c.Authenticator = gocql.PasswordAuthenticator{
			Username: *flagUser,
			Password: *flagPassword,
		}
	}
	fallback := gocql.RoundRobinHostPolicy()
	c.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(fallback)

	var (
		logger = log.NewDevelopment()
		ctx    = context.Background()
	)

	session, err := c.CreateSession()
	if err != nil {
		logger.Fatal(ctx, "Create session", "error", err)
	}

	// Create tables
	if *flagTables <= 0 {
		f := filler.NewFiller(session, *flagSize*1024*1024,
			int64(*flagPages*os.Getpagesize()), *flagParallel, logger)
		if err := f.Run(context.Background()); err != nil {
			logger.Fatal(ctx, "Run", "error", err)
		}
	} else {
		f := filler.NewMultiFiller(*flagTables, session, *flagSize*1024*1024, int64(*flagPages*os.Getpagesize()), *flagParallel, logger)
		if err := f.Run(context.Background()); err != nil {
			logger.Fatal(ctx, "Run", "error", err)
		}
	}
}
