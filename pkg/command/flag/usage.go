// Copyright (C) 2017 ScyllaDB

package flag

import (
	"bytes"
	"embed"
	"io/fs"
	"strings"

	"gopkg.in/yaml.v2"
)

var usage = make(map[string]string)

func init() {
	if err := parseFiles(); err != nil {
		panic(err)
	}
}

//go:embed *.yaml
var files embed.FS

func parseFiles() error {
	names, err := fs.Glob(files, "*.yaml")
	if err != nil {
		return err
	}
	for _, name := range names {
		b, err := files.ReadFile(name)
		if err != nil {
			return err
		}
		if err := yaml.Unmarshal(b, usage); err != nil {
			return err
		}
	}
	postProcessUsage()
	return nil
}

func postProcessUsage() {
	for k, v := range usage {
		v = strings.ReplaceAll(v, "${glob}", usage["glob"])
		v = cleanup(v)
		usage[k] = v
	}
}

func cleanup(s string) string {
	b := []byte(s)
	b = bytes.ReplaceAll(b, []byte{'`', '`'}, []byte{'\''})
	b = bytes.ReplaceAll(b, []byte{'\n', '*', ' '}, []byte{'\n'})
	// b = bytes.TrimSuffix(b, []byte{'\n'})
	return string(b)
}
