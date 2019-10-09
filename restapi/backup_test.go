// Copyright (C) 2017 ScyllaDB

package restapi_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/internal/timeutc"
	"github.com/scylladb/mermaid/restapi"
	"github.com/scylladb/mermaid/service/backup"
	"github.com/scylladb/mermaid/uuid"
)

//go:generate mockgen -destination mock_backupservice_test.go -mock_names BackupService=MockBackupService -package restapi github.com/scylladb/mermaid/restapi BackupService

func givenListBackupsRequest(clusterID uuid.UUID, host string, locations []backup.Location, filter backup.ListFilter) *http.Request {
	r := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/v1/cluster/%s/backups", clusterID.String()), nil)
	r.Form = url.Values{}
	r.Form.Add("host", host)
	for _, l := range locations {
		r.Form.Add("locations", l.String())
	}
	r.Form.Add("cluster_id", filter.ClusterID.String())
	r.Form["keyspace"] = filter.Keyspace
	a, _ := filter.MinDate.MarshalText()
	r.Form.Add("min_date", string(a))
	b, _ := filter.MaxDate.MarshalText()
	r.Form.Add("max_date", string(b))
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

		host      = "host"
		locations = []backup.Location{
			{Provider: backup.S3, Path: "foo"},
			{Provider: backup.S3, Path: "bar"},
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
				SnapshotTags: []string{"tag1"},
			},
		}
	)

	cm.EXPECT().GetCluster(gomock.Any(), cluster.ID.String()).Return(cluster, nil)
	bm.EXPECT().List(gomock.Any(), cluster.ID, host, locations, filter).Return(golden, nil)

	r := givenListBackupsRequest(cluster.ID, host, locations, filter)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	assertJsonBody(t, w, golden)
}
