// Copyright (C) 2017 ScyllaDB

package backup

import "testing"

func TestSStableID(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		in       string
		expected string
	}{
		{
			in:       "me-3g7k_098r_4wtqo2asamoc1i8h9n-big-CRC.db",
			expected: "3g7k_098r_4wtqo2asamoc1i8h9n",
		},
		{
			in:       "md-7-big-Statistics.db",
			expected: "7",
		},
		{
			in:       "mc-7-big-Statistics.db",
			expected: "7",
		},
		{
			in:       "me-7-big-TOC.txt",
			expected: "7",
		},
		{
			in:       "la-7-big-TOC.txt",
			expected: "7",
		},
		{
			in:       "keyspace1-standard1-ka-1-CRC.db",
			expected: "1",
		},
	}

	for id := range testCases {
		tcase := testCases[id]
		t.Run(tcase.in, func(t *testing.T) {
			t.Parallel()
			val := sstableID(tcase.in)
			if tcase.expected != val {
				t.Fatalf("expected %s, received %s", tcase.expected, val)
			}
		})
	}
}
