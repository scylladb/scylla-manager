// Copyright (C) 2017 ScyllaDB

package repair

import (
	"bytes"
	"fmt"
	"sort"
)

// dumpSegments writes segments as a coma separated list of pairs.
func dumpSegments(segments []*Segment) string {
	buf := bytes.Buffer{}

	first := true
	for _, s := range segments {
		if first {
			first = false
		} else {
			buf.WriteByte(',')
		}
		buf.WriteString(fmt.Sprintf("%d", s.StartToken))
		buf.WriteByte(':')
		buf.WriteString(fmt.Sprintf("%d", s.EndToken))
	}

	return buf.String()
}

// mergeSegments joins adjunct segments.
func mergeSegments(segments []*Segment) []*Segment {
	// handle empty segments
	if len(segments) == 0 {
		return segments
	}

	// sort
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].StartToken < segments[j].StartToken
	})

	var res []*Segment

	cur := segments[0]
	for i := 1; i < len(segments); i++ {
		// overlap
		if cur.EndToken >= segments[i].StartToken {
			if cur.EndToken < segments[i].EndToken {
				cur.EndToken = segments[i].EndToken
			}
			// don't overlap
		} else {
			res = append(res, cur)
			cur = segments[i]
		}
	}
	res = append(res, cur)

	return res
}

// splitSegments splits the segments so that each segment size is less or
// equal sizeLimit.
func splitSegments(segments []*Segment, sizeLimit int64) []*Segment {
	if len(segments) == 0 || sizeLimit <= 0 {
		return segments
	}

	// calculate slice size after the split
	size := int64(0)
	for _, s := range segments {
		r := s.EndToken - s.StartToken
		if r > sizeLimit {
			size += r / sizeLimit
		}
		size++
	}

	// no split needed
	if size == int64(len(segments)) {
		return segments
	}

	// split the segments
	split := make([]*Segment, 0, size)
	for _, s := range segments {
		r := s.EndToken - s.StartToken
		if r > sizeLimit {
			start := s.StartToken
			end := s.EndToken
			for start < end {
				token := start + sizeLimit
				if token > end {
					token = end
				}
				split = append(split, &Segment{StartToken: start, EndToken: token})
				start = token
			}
		} else {
			split = append(split, s)
		}
	}

	return split
}

// segmentsContainStartToken checks if there exists a segment starting with a
// given token.
func segmentsContainStartToken(segments []*Segment, token int64) (int, bool) {
	i := sort.Search(len(segments), func(i int) bool {
		return segments[i].StartToken >= token
	})
	if i < len(segments) && segments[i].StartToken == token {
		return i, true
	}

	return i, false
}
