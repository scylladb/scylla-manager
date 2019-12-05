// Copyright (C) 2017 ScyllaDB

package repair

type forwardIterator struct {
	segments          segments
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
			i.start, _ = i.segments.containStartToken(i.progress.LastStartToken)
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
	i.progress.SegmentError += i.end - i.start

	i.progress.SegmentErrorPos = appendRange(i.progress.SegmentErrorPos, i.start, i.end)
}

type repairIterator interface {
	Next() (start, end int, ok bool)
	OnSuccess()
	OnError()
}

type retryIterator struct {
	segments          segments
	progress          *RunProgress
	segmentsPerRepair int

	start int
	end   int
}

func (i *retryIterator) Next() (start, end int, ok bool) {
	if i.end != 0 {
		i.progress.SegmentErrorPos = i.progress.SegmentErrorPos[i.end-i.start:]
	}

	if len(i.progress.SegmentErrorPos) == 0 {
		return 0, 0, false
	}

	start = i.progress.SegmentErrorPos[0]
	if i.end != 0 && start <= i.start {
		return 0, 0, false
	}

	end = start + i.blockLength()
	if end > len(i.segments) {
		end = len(i.segments)
	}

	i.start = start
	i.end = end

	return i.start, i.end, true
}

// blockLength returns maximum number of segments that can be processed in
// parallel.
func (i *retryIterator) blockLength() int {
	c := 1
	for j := 1; j < len(i.progress.SegmentErrorPos) && j < i.segmentsPerRepair; j++ {
		if i.progress.SegmentErrorPos[j]-i.progress.SegmentErrorPos[j-1] > 1 {
			break
		}
		c++
	}
	return c
}

func (i *retryIterator) OnSuccess() {
	n := i.end - i.start
	i.progress.SegmentSuccess += n
	i.progress.SegmentError -= n
}

func (i *retryIterator) OnError() {
	i.progress.SegmentErrorPos = appendRange(i.progress.SegmentErrorPos, i.start, i.end)
}

func appendRange(ranges []int, start, end int) []int {
	for i := start; i < end; i++ {
		ranges = append(ranges, i)
	}
	return ranges
}
