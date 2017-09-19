// Copyright (C) 2017 ScyllaDB

package command

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/mitchellh/cli"
	"github.com/tonnerre/golang-text"
)

// maxLineLength is the maximum width of any line.
const maxLineLength int = 72

// BaseCommand handles common command behaviours.
type BaseCommand struct {
	UI cli.Ui

	flagSet *flag.FlagSet
	hidden  *flag.FlagSet
}

// NewFlagSet creates a new flag set for the given command. It automatically
// generates help output and adds the appropriate API flags.
func (cmd *BaseCommand) NewFlagSet(command cli.Command) *flag.FlagSet {
	f := flag.NewFlagSet("", flag.ContinueOnError)
	f.Usage = func() { cmd.UI.Error(command.Help()) }

	errR, errW := io.Pipe()
	errScanner := bufio.NewScanner(errR)
	go func() {
		for errScanner.Scan() {
			cmd.UI.Error(errScanner.Text())
		}
	}()
	f.SetOutput(errW)

	cmd.flagSet = f
	cmd.hidden = flag.NewFlagSet("", flag.ContinueOnError)

	return f
}

// HideFlags is used to set hidden flags that will not be shown in help text
func (cmd *BaseCommand) HideFlags(flags ...string) {
	for _, f := range flags {
		cmd.hidden.String(f, "", "")
	}
}

// Parse is used to parse the underlying flag set.
func (cmd *BaseCommand) Parse(args []string) error {
	return cmd.flagSet.Parse(args)
}

// Help returns the help for this flagSet.
func (cmd *BaseCommand) Help() string {
	// Some commands with subcommands call this without initializing
	// any flags first, so exit early to avoid a panic
	if cmd.flagSet == nil {
		return ""
	}
	return cmd.helpFlagsFor(cmd.flagSet)
}

// helpFlagsFor visits all flags in the given flag set and prints formatted
// help output.
func (cmd *BaseCommand) helpFlagsFor(f *flag.FlagSet) string {
	var out bytes.Buffer

	firstCommand := true
	f.VisitAll(func(f *flag.Flag) {
		if flagContains(cmd.hidden, f) {
			return
		}
		if firstCommand {
			printTitle(&out, "Command Options")
			firstCommand = false
		}
		printFlag(&out, f)
	})

	return strings.TrimRight(out.String(), "\n")
}

// printTitle prints a consistently-formatted title to the given writer.
func printTitle(w io.Writer, s string) {
	fmt.Fprintf(w, "%s\n\n", s)
}

// printFlag prints a single flag to the given writer.
func printFlag(w io.Writer, f *flag.Flag) {
	example, _ := flag.UnquoteUsage(f)
	if example != "" {
		fmt.Fprintf(w, "  -%s=<%s>", f.Name, example)
	} else {
		fmt.Fprintf(w, "  -%s", f.Name)
	}
	if f.DefValue != "" {
		fmt.Fprintf(w, " (default %v)", f.DefValue)
	}
	fmt.Fprint(w, "\n")

	indented := wrapAtLength(f.Usage, 5)
	fmt.Fprintf(w, "%s\n\n", indented)
}

// flagContains returns true if the given flag is contained in the given flag
// set or false otherwise.
func flagContains(fs *flag.FlagSet, f *flag.Flag) bool {
	var skip bool

	fs.VisitAll(func(hf *flag.Flag) {
		if skip {
			return
		}

		if f.Name == hf.Name {
			skip = true
			return
		}
	})

	return skip
}

// wrapAtLength wraps the given text at the maxLineLength, taking into account
// any provided left padding.
func wrapAtLength(s string, pad int) string {
	wrapped := text.Wrap(s, maxLineLength-pad)
	lines := strings.Split(wrapped, "\n")
	for i, line := range lines {
		lines[i] = strings.Repeat(" ", pad) + line
	}
	return strings.Join(lines, "\n")
}
