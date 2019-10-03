// Copyright (C) 2017 ScyllaDB

package clipper

import (
	"io"
	"strings"
)

var clipperPrefixes = []string{
	` __  `,
	`/  \ `,
	`@  @ `,
	`|  | `,
	`|| |/`,
	`|| ||`,
	`|\_/|`,
	`\___/`,
}
var clipperLength = len(clipperPrefixes[0])

// Say prints message with clipper asci art. Similar to `cowsay` on linux.
func Say(w io.Writer, lines ...string) error {
	const indent = "    "

	sb := strings.Builder{}
	for i, prefix := range clipperPrefixes {
		sb.WriteString(prefix)
		// message from second line of clipper
		if i != 0 && len(lines) > i-1 {
			sb.WriteString(indent)
			sb.WriteString(lines[i-1])
		}
		sb.WriteRune('\n')
	}
	for i := len(clipperPrefixes) - 1; i < len(lines); i++ {
		for k := 0; k < clipperLength; k++ {
			sb.WriteRune(' ')
		}
		sb.WriteString(indent)
		sb.WriteString(lines[i])
		sb.WriteRune('\n')
	}

	_, err := io.WriteString(w, sb.String())
	return err
}
