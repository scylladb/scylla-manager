# Fixing broken schema from Scylla Manager 3.1 release candidate

As described in this [issue](https://github.com/scylladb/scylla-manager/issues/3415), there was a time window
when instead of releasing Scylla Manager 3.1, we mistakenly released the release candidate. \
It means that anyone, who upgraded Scylla Manger to version 3.1 shortly after its release,
could experience various bugs (e.g. broken backup retention which could lead to growing storage space consumption).

## Verifying Scylla Manager version

In order to determine, whether you have installed the release candidate, you can run:

```
sctool version
```

If you see:

```
Client version: 3.1.0-rc0-0.20230116.66e64b43b40
Server version: 3.1.0-rc0-0.20230116.66e64b43b40
```

It means that you are running the release candidate. If that's not the case, you have the correct version.


## Fixing Scylla Manager version

Unfortunately, it's not possible to simply reinstall Scylla Manager or to upgrade it to a newer version.
If you try to do that, you will see the following error:
```
STARTUP ERROR: db init: file "v3.1.0.cql" was tempered with, expected md5 4c5ffbf3ca2af17116b3dcbeca8bf7d4
```
That's because the schema of Scylla Manager database has been changed between release candidate and the proper version. \
In order to fix that, schema changes need to be applied manually via `cqlsh` on the Scylla Manager's database.

The overall procedure is mostly identical to the regular upgrade procedure (for more details see [docs](https://manager.docs.scylladb.com/stable/upgrade/#upgrade-procedure)):
1. Stop all Scylla Manager tasks (or wait for them to finish)
2. Stop the Scylla Manager Server
3. Stop the Scylla Manager Agent on all nodes
4. Upgrade or reinstall Scylla Manager Server and Client
5. Upgrade or reinstall Scylla Manager Agent on all nodes
6. Run *scyllamgr_agent_setup* script on all nodes
7. Reconcile configuration files
8. Check Scylla Manager keyspace name (default name is: *scylla_manager*) - it is stored in the *scylla-manager.yaml* file (located in */etc/scylla-manager/scylla-manager.yaml* by default).
9. Execute following statements via `cqlsh` on the Scylla Manager's database (all credentials required by `cqlsh` are also stored in the *scylla-manager.yaml* file):
   ```
   USE <scylla-manager-keyspace>;
   ALTER TABLE restore_run_progress ADD versioned_progress bigint;
   ALTER TABLE restore_run ADD location text;
   UPDATE gocqlx_migrate SET checksum = '7cb98d445928959e06fcb9f893f10ad4' WHERE name = 'v3.1.0.cql';
   ```
   Or you can simply run those statements from the *missing_schema_v3.1.0.cql* file (remember to change used keyspace if it's not the default one):
   ```
   cqlsh -f missing_schema_v3.1.0.cql
   ```
10. Start upgraded or reinstalled Scylla Manager Agent on all nodes
11. Start upgraded or reinstalled Scylla Manager Server
12. Validate status of the cluster

## Result

Now you should have the correct version of Scylla Manager. \
It's important to note that procedure described above:
- preserves all Scylla Manager's tasks
- does not invalidate any stored backups 

This means that there is nothing else that needs to be taken care of.
