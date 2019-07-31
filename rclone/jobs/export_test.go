// Copyright (C) 2017 ScyllaDB

package jobs

// Used in tests.
func (jobs *Jobs) cleanup() {
	jobs.mu.RLock()
	defer jobs.mu.RUnlock()

	for _, job := range jobs.jobs {
		job.Stop()
	}
	jobs.jobs = make(map[string]*Job)
}
