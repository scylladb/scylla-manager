package main

import (
	"fmt"
	"log"
	"path/filepath"
)

func main() {
	matches, err := filepath.Glob("./scenarios/*")
	if err != nil {
		log.Fatal(err)
	}
	for _, match := range matches {
		fmt.Println(match)
	}
}
