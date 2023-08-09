// Copyright (C) 2017 ScyllaDB

package restapi_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/restapi"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/service/scheduler"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
)

//go:generate mockgen -destination mock_backupservice_test.go -mock_names BackupService=MockBackupService -package restapi github.com/scylladb/scylla-manager/v3/pkg/restapi BackupService
//go:generate mockgen -destination mock_schedservice_test.go -mock_names SchedService=MockSchedService -package restapi github.com/scylladb/scylla-manager/v3/pkg/restapi SchedService

func listBackupsRequest(clusterID uuid.UUID) *http.Request {
	return httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/cluster/%s/backups", clusterID.String()), nil)
}

func listBackupsPurgeRequest(clusterID uuid.UUID) *http.Request {
	return httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/v1/cluster/%s/backups/purge", clusterID.String()), nil)
}

func listBackupFilesRequest(clusterID uuid.UUID) *http.Request {
	return httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/cluster/%s/backups/files", clusterID.String()), nil)
}

func withForm(r *http.Request, dryRun bool, locations []backupspec.Location, filter backup.ListFilter, query string) *http.Request {
	r.Form = url.Values{}
	for _, l := range locations {
		r.Form.Add("locations", l.String())
	}
	r.Form.Add("cluster_id", filter.ClusterID.String())
	r.Form.Add("query_cluster_id", query)
	r.Form["keyspace"] = filter.Keyspace
	r.Form.Add("snapshot_tag", filter.SnapshotTag)
	a, _ := filter.MinDate.MarshalText()
	r.Form.Add("min_date", string(a))
	b, _ := filter.MaxDate.MarshalText()
	r.Form.Add("max_date", string(b))
	r.Form.Add("dry_run", fmt.Sprintf("%v", dryRun))

	return r
}

func TestBackupList(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cm := restapi.NewMockClusterService(ctrl)
	bm := restapi.NewMockBackupService(ctrl)

	services := restapi.Services{
		Cluster: cm,
		Backup:  bm,
	}

	h := restapi.New(services, log.Logger{})

	var (
		cluster = givenCluster()

		locations = []backupspec.Location{
			{Provider: backupspec.S3, Path: "foo"},
			{Provider: backupspec.S3, Path: "bar"},
		}
		filter = backup.ListFilter{
			ClusterID: cluster.ID,
			Keyspace:  []string{"keyspace1", "keyspace2"},
			MinDate:   timeutc.Now(),
			MaxDate:   timeutc.Now(),
		}

		golden = []backup.ListItem{
			{
				ClusterID: filter.ClusterID,
				Units: []backup.Unit{
					{
						Keyspace: "keyspace1",
						Tables:   []string{"table1"},
					},
				},
				SnapshotInfo: []backup.SnapshotInfo{{SnapshotTag: "tag1"}},
			},
		}
	)

	cm.EXPECT().GetCluster(gomock.Any(), cluster.ID.String()).Return(cluster, nil)
	bm.EXPECT().List(gomock.Any(), cluster.ID, locations, filter).Return(golden, nil)

	r := withForm(listBackupsRequest(cluster.ID), false, locations, filter, cluster.Name)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	assertJsonBody(t, w, golden)
}

func TestBackupPurge(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cm := restapi.NewMockClusterService(ctrl)
	bm := restapi.NewMockBackupService(ctrl)
	sm := restapi.NewMockSchedService(ctrl)

	services := restapi.Services{
		Cluster:   cm,
		Backup:    bm,
		Scheduler: sm,
	}

	h := restapi.New(services, log.NewDevelopment())

	var (
		cluster = givenCluster()

		locations = backupspec.Locations{
			{Provider: backupspec.S3, Path: "foo"},
			{Provider: backupspec.S3, Path: "bar"},
		}

		taskID1 = uuid.MustRandom()
		taskID2 = uuid.MustRandom()

		snapshotNow1 = backupspec.SnapshotTagAt(time.Now())
		snapshotNow2 = backupspec.SnapshotTagAt(time.Now().Add(time.Minute))

		filter = backup.ListFilter{
			ClusterID: cluster.ID,
		}

		manifests = backupspec.Manifests{
			{
				Location:    locations[0],
				DC:          "",
				ClusterID:   cluster.ID,
				NodeID:      "",
				TaskID:      taskID1,
				SnapshotTag: snapshotNow1,
				Temporary:   false,
			},
			{
				Location:    locations[1],
				DC:          "",
				ClusterID:   cluster.ID,
				NodeID:      "",
				TaskID:      taskID2,
				SnapshotTag: snapshotNow2,
				Temporary:   false,
			},
		}
		expected = restapi.BackupPurgeOut{
			Warnings: make([]string, 0),
		}
	)

	task1 := makeTaskItem(cluster.ID, taskID1, locations, 3, 30)
	task2 := makeTaskItem(cluster.ID, taskID2, locations, 3, 30)
	tasks := []*scheduler.TaskListItem{
		task1, task2,
	}
	cm.EXPECT().GetCluster(gomock.Any(), cluster.ID.String()).Return(cluster, nil)
	sm.EXPECT().ListTasks(gomock.Any(), cluster.ID, scheduler.ListFilter{TaskType: []scheduler.TaskType{scheduler.BackupTask}}).Return(tasks, nil)
	bm.EXPECT().PurgeBackups(gomock.Any(), cluster.ID, locations, gomock.Any(), gomock.Any()).Return(manifests, expected.Warnings, nil)

	expected.Deleted = restapi.ConvertManifestsToListItems(manifests)
	r := withForm(listBackupsPurgeRequest(cluster.ID), true, locations, filter, cluster.Name)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	assertJsonBody(t, w, expected)
}

func makeTaskItem(clusterID, taskID uuid.UUID, locations backupspec.Locations, retention, day int) *scheduler.TaskListItem {
	taskProp := backup.TaskProperties{
		Location:     locations,
		RetentionMap: make(backup.RetentionMap, 0),
	}
	taskProp.RetentionMap.Add(taskID, retention, day)

	prop, err := json.Marshal(taskProp)
	if err != nil {
		return nil
	}

	return &scheduler.TaskListItem{
		Task: scheduler.Task{
			ClusterID:  clusterID,
			ID:         taskID,
			Type:       scheduler.BackupTask,
			Enabled:    true,
			Sched:      scheduler.Schedule{StartDate: time.Now()},
			Properties: prop,
		},
		Suspended:      false,
		NextActivation: nil,
		Retry:          0,
	}

}

func TestBackupListAllClusters(t *testing.T) {

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cm := restapi.NewMockClusterService(ctrl)
	bm := restapi.NewMockBackupService(ctrl)

	services := restapi.Services{
		Cluster: cm,
		Backup:  bm,
	}

	h := restapi.New(services, log.Logger{})

	var (
		cluster = givenCluster()

		locations = []backupspec.Location{
			{Provider: backupspec.S3, Path: "foo"},
			{Provider: backupspec.S3, Path: "bar"},
		}
		filter = backup.ListFilter{
			Keyspace: []string{"keyspace1", "keyspace2"},
			MinDate:  timeutc.Now(),
			MaxDate:  timeutc.Now(),
		}

		golden = []backup.ListItem{
			{
				ClusterID: cluster.ID,
				Units: []backup.Unit{
					{
						Keyspace: "keyspace1",
						Tables:   []string{"table1"},
					},
				},
				SnapshotInfo: []backup.SnapshotInfo{{SnapshotTag: "tag1"}},
			},
		}
	)

	cm.EXPECT().GetCluster(gomock.Any(), cluster.ID.String()).Return(cluster, nil)
	bm.EXPECT().List(gomock.Any(), cluster.ID, locations, filter).Return(golden, nil)

	r := withForm(listBackupsRequest(cluster.ID), false, locations, filter, "")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	assertJsonBody(t, w, golden)
}

func TestBackupListFiles(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cm := restapi.NewMockClusterService(ctrl)
	bm := restapi.NewMockBackupService(ctrl)

	services := restapi.Services{
		Cluster: cm,
		Backup:  bm,
	}

	h := restapi.New(services, log.Logger{})

	var (
		cluster = givenCluster()

		locations = []backupspec.Location{
			{Provider: backupspec.S3, Path: "foo"},
			{Provider: backupspec.S3, Path: "bar"},
		}
		filter = backup.ListFilter{
			ClusterID:   cluster.ID,
			Keyspace:    []string{"keyspace1", "keyspace2"},
			SnapshotTag: "tag",
		}

		golden = []backupspec.FilesInfo{
			{
				Location: locations[0],
				Schema:   "schema",
			},
			{
				Location: locations[1],
				Schema:   "schema",
			},
		}
	)

	cm.EXPECT().GetCluster(gomock.Any(), cluster.ID.String()).Return(cluster, nil)
	bm.EXPECT().ListFiles(gomock.Any(), cluster.ID, locations, filter).Return(golden, nil)

	r := withForm(listBackupFilesRequest(cluster.ID), false, locations, filter, cluster.ID.String())
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	assertJsonBody(t, w, golden)
}
