package main

import (
	"fmt"
	"syscall"
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(rootCmd.OutOrStderr(), "\nSTARTUP ERROR: %s\n\n", err)
		syscall.Exit(1)
	}
}
