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
	"github.com/scylladb/mermaid/pkg/restapi"
	"github.com/scylladb/mermaid/pkg/service"
	"github.com/scylladb/mermaid/pkg/util/uuid"
)

//go:generate mockgen -destination mock_repairservice_test.go -mock_names RepairService=MockRepairService -package restapi github.com/scylladb/mermaid/pkg/restapi RepairService

func updateIntensityRequest(clusterID uuid.UUID, intensity float64) *http.Request {
	r := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/v1/cluster/%s/repairs/intensity", clusterID.String()), nil)
	r.Form = url.Values{}
	r.Form.Add("intensity", fmt.Sprintf("%f", intensity))

	return r
}

func TestRepairSetIntensity(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	cm := restapi.NewMockClusterService(ctrl)
	rm := restapi.NewMockRepairService(ctrl)

	services := restapi.Services{
		Cluster: cm,
		Repair:  rm,
	}

	h := restapi.New(services, log.Logger{})

	var (
		cluster = givenCluster()

		intensity = float64(50)
	)

	t.Run("successful update", func(t *testing.T) {
		cm.EXPECT().GetCluster(gomock.Any(), cluster.ID.String()).Return(cluster, nil)
		rm.EXPECT().SetIntensity(gomock.Any(), cluster.ID, intensity).Return(nil)

		w := httptest.NewRecorder()
		r := updateIntensityRequest(cluster.ID, intensity)
		h.ServeHTTP(w, r)
		rs := w.Result()
		if rs.StatusCode != http.StatusOK {
			t.Errorf("wrong status code, got %d, expected %d", w.Result().StatusCode, http.StatusOK)
		}
	})

	t.Run("not found when service returns not found error", func(t *testing.T) {
		cm.EXPECT().GetCluster(gomock.Any(), cluster.ID.String()).Return(cluster, nil)
		rm.EXPECT().SetIntensity(gomock.Any(), cluster.ID, intensity).Return(service.ErrNotFound)

		r := updateIntensityRequest(cluster.ID, intensity)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Result().StatusCode != http.StatusNotFound {
			t.Errorf("wrong status code, got %d, expected %d", w.Result().StatusCode, http.StatusNotFound)
		}
	})
}
