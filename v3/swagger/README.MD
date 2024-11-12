This module contains the following swagger definitions used in *Scylla Manager* repository:

1. **scylla-manager** - describes *Scylla Manager* API. It is mainly used by the *sctool* CLI and *Scylla Cloud* and *Scylla Operator* teams.

2. **scylla-manager-agent** - describes *Scylla Manager Agent* API. It is mainly used by the *Scylla Manager*.
It consists mostly of the *rclone* server swagger definitions, but there are also a few agent specific definitions as well.

3. **scylla_v1** - describes general *Scylla* API. The swagger definitions come from [Scylla repository](https://github.com/scylladb/scylladb/tree/master/api),
and they are divided into per service files (e.g. [storage_service](https://github.com/scylladb/scylladb/blob/b158ca734603d62fa8e173a72e60866a4d8d3bc7/api/api-doc/storage_service.json#L5)).
Note that *Scylla* uses swagger *1.2*, and we use swagger *2.0* in Scylla Manager repository, so the swagger definitions needs to be adjusted accordingly.

4. **scylla_v2** - describes config related *Scylla* API. The swagger definitions [correspond to querying](https://github.com/scylladb/scylladb/blob/b158ca734603d62fa8e173a72e60866a4d8d3bc7/api/api-doc/config.json#L1)
*Scylla* node's *scylla.yaml* config file. Note that in some cases this API does not distinguish [unset and default values](https://github.com/scylladb/scylladb/issues/7206#issuecomment-689598343).