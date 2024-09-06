// Copyright (C) 2017 ScyllaDB

// Copyright 2010 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// This file contains a modified version of time.Duration String function from
// go stdlib src/time/duration.go file together with all required helper functions.

package duration

import "time"

// String is a modified time.Duration String function that supports printing
// days with d suffix, note that day is equal to 24h. This is not supported by
// time.Duration because of issues with leap year and different time zones. We
// decide to ignore that as we are working with UTC.
//
// The implementation is changed not to print units if value is 0 to keep the
// representation shorter i.e. instead of 7d0h0m0s we print just 7d.
//
// String returns a string representing the duration in the form "72h3m0.5s".
// Leading zero units are omitted. As a special case, durations less than one
// second format use a smaller unit (milli-, micro-, or nanoseconds) to ensure
// that the leading digit is non-zero. The zero duration formats as 0s.
func (d Duration) String() string {
	// Largest time is 2540400h10m10.000000000s
	var buf [32]byte
	w := len(buf)

	u := uint64(d)
	neg := d < 0
	if neg {
		u = -u
	}

	if u < uint64(time.Second) {
		// Special case: if duration is smaller than a second,
		// use smaller units, like 1.2ms
		var prec int
		w--
		buf[w] = 's'
		w--
		switch {
		case u == 0:
			return "0s"
		case u < uint64(time.Microsecond):
			// print nanoseconds
			prec = 0
			buf[w] = 'n'
		case u < uint64(time.Millisecond):
			// print microseconds
			prec = 3
			// U+00B5 'µ' micro sign == 0xC2 0xB5
			w-- // Need room for two bytes.
			copy(buf[w:], "µ")
		default:
			// print milliseconds
			prec = 6
			buf[w] = 'm'
		}
		w, u = fmtFrac(buf[:w], u, prec)
		w = fmtInt(buf[:w], u)
	} else {
		w--
		buf[w] = 's'

		w, u = fmtFrac(buf[:w], u, 9)

		// u is now integer seconds
		if u%60 != 0 || w != len(buf)-1 {
			w = fmtInt(buf[:w], u%60)
		} else {
			w = len(buf)
		}
		u /= 60

		// u is now integer minutes
		if u > 0 {
			if u%60 != 0 {
				w--
				buf[w] = 'm'
				w = fmtInt(buf[:w], u%60)
			}
			u /= 60

			// u is now integer hours
			if u > 0 {
				if u%24 != 0 {
					w--
					buf[w] = 'h'
					w = fmtInt(buf[:w], u%24)
				}
				u /= 24

				// u is now integer days
				if u > 0 {
					w--
					buf[w] = 'd'
					w = fmtInt(buf[:w], u)
				}
			}
		}
	}

	if neg {
		w--
		buf[w] = '-'
	}

	return string(buf[w:])
}

// fmtFrac formats the fraction of v/10**prec (e.g., ".12345") into the
// tail of buf, omitting trailing zeros. It omits the decimal
// point too when the fraction is 0. It returns the index where the
// output bytes begin and the value v/10**prec.
func fmtFrac(buf []byte, v uint64, prec int) (nw int, nv uint64) {
	// Omit trailing zeros up to and including decimal point.
	w := len(buf)
	print := false
	for i := 0; i < prec; i++ {
		digit := v % 10
		print = print || digit != 0
		if print {
			w--
			buf[w] = byte(digit) + '0'
		}
		v /= 10
	}
	if print {
		w--
		buf[w] = '.'
	}
	return w, v
}

// fmtInt formats v into the tail of buf.
// It returns the index where the output begins.
func fmtInt(buf []byte, v uint64) int {
	w := len(buf)
	if v == 0 {
		w--
		buf[w] = '0'
	} else {
		for v > 0 {
			w--
			buf[w] = byte(v%10) + '0'
			v /= 10
		}
	}
	return w
}
