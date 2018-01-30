// Copyright (C) 2017 ScyllaDB

package repair

type repairIterator interface {
	Next() (start, end int, ok bool)
	OnSuccess()
	OnError()
}

type retryIterator struct {
	segments          []*Segment
	progress          *RunProgress
	segmentsPerRepair int

	start int
	end   int
}

func (i *retryIterator) Next() (start, end int, ok bool) {
	if i.end != 0 {
		i.progress.SegmentErrorStartTokens = i.progress.SegmentErrorStartTokens[1:]
	}

	if len(i.progress.SegmentErrorStartTokens) == 0 {
		return 0, 0, false
	}

	startToken := i.progress.SegmentErrorStartTokens[0]
	start, _ = segmentsContainStartToken(i.segments, startToken)
	if i.end != 0 && start <= i.start {
		return 0, 0, false
	}
	i.start = start
	i.end = i.start + i.segmentsPerRepair

	if i.end > len(i.segments) {
		i.end = len(i.segments)
	}
	return i.start, i.end, true
}

func (i *retryIterator) OnSuccess() {
	n := i.end - i.start
	i.progress.SegmentSuccess += n
	i.progress.SegmentError -= n
}

func (i *retryIterator) OnError() {
	startToken := i.progress.SegmentErrorStartTokens[0]

	i.progress.SegmentErrorStartTokens = append(i.progress.SegmentErrorStartTokens, startToken)
}

type forwardIterator struct {
	segments          []*Segment
	progress          *RunProgress
	segmentsPerRepair int

	start int
	end   int
}

func (i *forwardIterator) Next() (start, end int, ok bool) {
	if i.end == len(i.segments) {
		return 0, 0, false
	}

	if i.end == 0 {
		if i.progress.LastStartToken != 0 {
			i.start, _ = segmentsContainStartToken(i.segments, i.progress.LastStartToken)
		}
		i.end = i.start + i.segmentsPerRepair
	} else {
		i.start, i.end = i.end, i.end+i.segmentsPerRepair
	}

	if i.end > len(i.segments) {
		i.end = len(i.segments)
	}
	return i.start, i.end, true
}

func (i *forwardIterator) OnSuccess() {
	i.progress.SegmentSuccess += i.end - i.start
}

func (i *forwardIterator) OnError() {
	startToken := i.segments[i.start].StartToken

	i.progress.SegmentError += i.end - i.start
	i.progress.SegmentErrorStartTokens = append(i.progress.SegmentErrorStartTokens, startToken)
}
