// Copyright (C) 2017 ScyllaDB

package main

import (
	"io"
	"log"
	"net/http"
	"os"
)

func main() {
	server := http.Server{
		Handler: dispatcher{http.DefaultClient},
	}

	if err := server.Serve(newListener(os.Stdout, os.Stdin)); err != nil {
		if err != io.EOF {
			log.Fatalln(err)
		}
	}
}
