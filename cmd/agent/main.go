// Copyright (C) 2017 ScyllaDB

package main

import (
	"io"
	"log"
	"net/http"
	"os"

	"github.com/scylladb/mermaid/cmd/agent/rclone"
)

func main() {
	server := http.Server{
		Handler: newRouter(
			defaultConfig(),
			rclone.NewHandler(),
			http.DefaultClient,
		),
	}

	l := newListener(os.Stdout, os.Stdin)
	if err := server.Serve(l); err != nil {
		if err != io.EOF {
			log.Fatalln(err)
		}
	}
}
