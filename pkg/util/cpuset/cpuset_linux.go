// Copyright (C) 2017 ScyllaDB
// +build linux

package cpuset

// AvailableCPUs returns a list of CPUs of length wantCPUs that does not contain
// any of the busyCPUs. If the conditions cannot be met error is returned.
// AvailableCPUs selects the CPUs with the highest available indexes to offload
// shard 0...
func AvailableCPUs(busyCPUs []int) ([]int, error) {
	var cpus unix.CPUSet
	if err := unix.SchedGetaffinity(os.Getpid(), &cpus); err != nil {
		return nil, errors.Wrap(err, "get affinity")
	}
	for _, c := range busyCPUs {
		cpus.Clear(c)
	}

	n := cpus.Count()
	if n == 0 {
		return nil, errors.Errorf("no available CPUs")
	}

	return cpulist(&cpus), nil
}

// SchedSetAffinity makes all of this process threads run on the provided set
// of CPUs only.
func SchedSetAffinity(useCPUs []int) error {
	pids, err := osTasks(os.Getpid())
	if err != nil {
		return errors.Wrap(err, "get tasks")
	}

	return schedSetAffinityToMany(pids, cpuset(useCPUs))
}

func schedSetAffinityToMany(pids []int, set *unix.CPUSet) (err error) {
	for _, pid := range pids {
		err = multierr.Append(unix.SchedSetaffinity(pid, set), errors.Wrapf(err, "pid %d", pid))
	}
	return
}

func cpuset(cpus []int) *unix.CPUSet {
	set := &unix.CPUSet{}
	set.Zero()
	for _, c := range cpus {
		set.Set(c)
	}
	return set
}

func cpulist(set *unix.CPUSet) []int {
	var cpus []int

	n := set.Count()
	for i := 0; len(cpus) < n; i++ {
		if set.IsSet(i) {
			cpus = append(cpus, i)
		}
	}

	return cpus
}

func osTasks(pid int) ([]int, error) {
	files, err := ioutil.ReadDir(fmt.Sprint("/proc/", pid, "/task"))
	if err != nil {
		return nil, err
	}

	var pids []int
	for _, f := range files {
		p, _ := strconv.ParseInt(f.Name(), 10, 64) // nolint: errcheck
		pids = append(pids, int(p))
	}

	return pids, nil
}
