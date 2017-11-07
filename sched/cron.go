// Copyright (C) 2017 ScyllaDB

// +build go1.9

package sched

import (
	"container/heap"
	"time"
)

type trigger struct {
	At time.Time
	F  func(now time.Time)
}

// taskSched is an in-process task scheduler
type taskSched struct {
	timer  *time.Timer
	tasks  triggerHeap
	req    chan *trigger
	cancel chan *trigger
	close  chan struct{}
}

func newTaskSched() *taskSched {
	return &taskSched{
		tasks:  make(triggerHeap, 0, 128),
		req:    make(chan *trigger, 16),
		cancel: make(chan *trigger, 16),
		close:  make(chan struct{}),
	}
}

func (sched *taskSched) sched() {
	var timerC <-chan time.Time

	rearm := func() {
		timerC = nil
		t := sched.nextTrigger()
		if t == nil {
			return
		}

		now := timeNow().UTC()
		d := t.At.Sub(now)
		if sched.timer == nil {
			sched.timer = time.NewTimer(d)
		} else {
			sched.timer.Reset(d)
		}
		timerC = sched.timer.C
	}

	for {
		select {
		case newTask := <-sched.req:
			if timerC != nil && !sched.timer.Stop() {
				<-timerC
			}
			heap.Push(&sched.tasks, newTask)
			rearm()

		case cancelTask := <-sched.cancel:
			if timerC != nil && !sched.timer.Stop() {
				<-timerC
			}
			for i, t := range sched.tasks {
				if t == cancelTask {
					heap.Remove(&sched.tasks, i)
					break
				}
			}
			rearm()

		case now := <-timerC:
			if now.After(sched.tasks[0].At) {
				t := heap.Pop(&sched.tasks).(*trigger)
				go t.F(now)
			}
			rearm()

		case <-sched.close:
			sched.timer.Stop()
			return
		}
	}
}

func (sched *taskSched) nextTrigger() *trigger {
	if len(sched.tasks) == 0 {
		return nil
	}
	return sched.tasks[0]
}

func (sched *taskSched) Start() {
	go sched.sched()
}

func (sched *taskSched) Stop() {
	close(sched.close)
}

func (sched *taskSched) Add(at time.Time, f func(time.Time)) *trigger {
	// not using at directly to ensure it has a monotonic part
	now := timeNow().UTC()
	diff := at.Sub(now)
	t := &trigger{At: now.Add(diff), F: f}
	sched.req <- t
	return t
}

func (sched *taskSched) Cancel(t *trigger) {
	sched.cancel <- t
}

type triggerHeap []*trigger

func (h triggerHeap) Len() int           { return len(h) }
func (h triggerHeap) Less(i, j int) bool { return h[i].At.Before(h[j].At) }
func (h triggerHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *triggerHeap) Push(x interface{}) {
	*h = append(*h, x.(*trigger))
}

func (h *triggerHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return x
}
