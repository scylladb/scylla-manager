// Copyright (C) 2017 ScyllaDB

package main

const clipper = ` __
/  \     Cluster added, to set it as a default run:
@  @     export SCYLLA_MANAGER_CLUSTER=%s
|  |
|| |/    Repair will run on %s and will be repeated every %s.
|| ||    To see the currently scheduled tasks: sctool task list -c %s
|\_/|
\___/

`
