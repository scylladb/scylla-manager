package main

import (
	"flag"
	"go/format"
	"go/parser"
	"go/token"
	"log"
	"os"

	"golang.org/x/tools/go/ast/astutil"
)

func main() {
	var (
		file = flag.String("file", "", "path to file to fix")
		imp  = flag.String("import", "", "import to add")
	)
	flag.Parse()

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, *file, nil, parser.ParseComments)
	if err != nil {
		log.Fatal(err)
	}
	if !astutil.AddImport(fset, f, *imp) {
		return
	}

	out, err := os.Create(*file)
	if err != nil {
		log.Fatal(err)
	}
	if err := format.Node(out, fset, f); err != nil {
		log.Fatal(err)
	}
	if err := out.Close(); err != nil {
		log.Fatal(err)
	}
}
