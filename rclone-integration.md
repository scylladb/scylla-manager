# Rclone integration

Scylla manager agent depends on [rclone](https://rclone.org/) for all file based operations.
This includes both local and remote operations like access to AWS S3 service. 
Main purpose of rclone is to be used as a CLI tool, but agent is using it as a library.
This sometimes creates problems when agent is using internals of rclone that are not intended for library usage.

Our approach to fixing these problems is to primarily look for a way to contribute fixes upstream to the rclone.
If fixing stuff upstream is not possible then we are taking parts of rclone and adapt them to be used as internal packages of the manager.
If adaptation is not possible then fixes/improvements are added to the rclone fork that we maintain.

## Upstream contribution

Main maintainer of the rclone project is Nick Craig-Wood:

Email: nick@craig-wood.com
Github: github.com/ncw

He is receptive of reasonable requests, but only favors things that fixes existing issues or improve UX of the general users.
We had push backs on contributions that only relate to internal changes that don't have immediate effect on CLI users like code API changes.

General discussion that doesn't discuss code it self should be started on the [rclone forum](https://forum.rclone.org/).

Experience so far shows that quick fixes can be submitted as pull requests with sufficient explanation of the purpose for the change.
As long as the change is obvious enough and passes the tests there should be large chance that it will be accepted.

## Rclone adaptations

All internal adaptations of rclone are located at `pkg/rclone`.

Major adaptation is rclone's HTTP server.
HTTP server is copied from the rclone and cleaned up for the agent's usage.
This is because it represents main entry point to the rclone environment.
Rclone environment is mostly based on global singletons due to CLI fire and forget architecture.
So all environment initialization is placed around internal rcserver adaptation.
Server also needed additional validation steps, better error and response handling, logging, metrics and other minor fixes.
As a important component of the agent it will remain part of the manager project.

Other notable adaptations:

- local backend is replaced with the one that is containing file operations to the Scylla data directory.
- Some operations like Cat are adapted and CheckPermissions are added located at `pkg/rclone/operations`
- New calls like job/info, operations/check-permissions, operations/put and overrides to rclone calls are added.

## Rclone fork

In order to contribute any improvements and fixes back to the upstream project we are using [rclone fork on scylladb](https://github.com/scylladb/rclone).
But rclone is constantly changing.
To make it more predictable we are using patched versions of the rclone official releases which are maintained by us.
These versions are maintained on special branches that are kept in the rclone fork.

Scylla manager versions 2.0.* are based on patched v1.50.2 version of rclone.
Branch name for this patch is `v1.50.2-patched`.

Scylla manager versions 2.1.* are based on patched v1.51.0 version of rclone.
Branch name for this patch is `v1.51.0-patched`.

New contribution that can't be accepted upstream goes into it's own branch on the fork and then it's cherry picked to the patched branch.
When branch is ready for official release commit is tagged with `v<version>-patched-<patch_increment>` format.

### Go and versioning issues

Fork overrides default `github/rclone/rclone` dependency for example:

    github.com/rclone/rclone => github.com/scylladb/rclone v1.51.0-patched-4

Because of go mod hashing it is not possible/easy to override version so new version increment has to be added in order to pull force pushed changes to the patched branch.
This has led to patch number inflation in the past so there could be ranges of patch numbers that are not used.

To remedy this it is advised to use absolute path to the local rclone fork that is underdevelopment for testing before creating patched version.  