package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"html/template"
	"os"
	"path"
)

func main() {
	if len(os.Args) < 2 {
		fail("Missing template filename", nil)
	}

	dec := json.NewDecoder(bufio.NewReader(os.Stdin))

	t := template.Must(template.New(path.Base(os.Args[1])).ParseFiles(os.Args[1]))

	var m []string
	if err := dec.Decode(&m); err != nil {
		fail("input decoding", err)
	}

	if err := t.Execute(os.Stdout, m); err != nil {
		fail("executing template", err)
	}
}

func fail(msg string, err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %+v\n", msg, err)
	} else {
		fmt.Fprintln(os.Stderr, msg)
	}
	os.Exit(1)
}
