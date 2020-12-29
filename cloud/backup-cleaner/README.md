# Backup cleaner

This is an instruction on manually removing old and orphaned files from Scylla Manager backup.
It will remove:
* All manifests that are older than a month
* All files that do not belong to any manifest

You will need access to the backup location of a cluster.
You can use one of the cluster nodes.

1. Install ncdu-s3

```bash
python3 -m venv venv
source venv/bin/activate
git clone https://github.com/mmatczuk/ncdu-s3.git
pip install ./ncdu-s3
```

2. Run it

If running on a node use `scylla-helper.slice`.

```bash
sudo systemd-run --slice scylla-helper.slice venv/bin/ncdu-s3 s3://scylla-cloud-backup-XXXX /home/support/ncdu.out
```

3. Copy manifests

```bash
aws s3 cp --recursive s3://scylla-cloud-backup-XXXX/backup/meta meta
```

4. Package it and transfer to localhost

```bash
tar -czvf <cluster_name>.tgz ncdu.out meta/
```

5. Unpack it and run main.go

```bash
go run . -dir <directory where ncdu.out is>
```

In the directory there are 3 files created:

* `summary` - a copy of the summary printed out on screen
* `report.csv` - each orphaned file with its date and time as unix timestamp in cvs format
* `deletes-to-exec` input to `delete.sh`

6. Transfer `deletes-to-exec` and `delete.sh` to a machine with access to the bucket

7. Run the delete script

If running on a node use `scylla-helper.slice`.

```
sudo systemd-run --slice scylla-helper.slice ./delete.sh scylla-cloud-backup-XXXX $(pwd)/deletes-to-exec
```

8. Validate the deletes were successful

Look for errors `grep Errors -A 30 ./delete.sh.log`.
Check the object count matches what we have in summary `grep Key ./delet.sh.log | wc -l`. 
