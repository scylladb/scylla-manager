// Copyright (C) 2017 ScyllaDB

package restapi_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/cluster"
	"github.com/scylladb/mermaid/log"
	"github.com/scylladb/mermaid/mermaidmock"
	"github.com/scylladb/mermaid/repair"
	"github.com/scylladb/mermaid/restapi"
	"github.com/scylladb/mermaid/uuid"
)

var (
	uuid1, uuid2, uuid3 uuid.UUID
)

func init() {
	uuid1.UnmarshalText([]byte("00000000-0000-0000-0000-000000000001"))
	uuid2.UnmarshalText([]byte("00000000-0000-0000-0000-000000000002"))
	uuid3.UnmarshalText([]byte("00000000-0000-0000-0000-000000000003"))
}

type repairProgress struct {
	Keyspace        string                 `json:"keyspace"`
	Tables          []string               `json:"tables"`
	Status          repair.Status          `json:"status"`
	PercentComplete int                    `json:"percent_complete"`
	Total           int                    `json:"total"`
	Success         int                    `json:"success"`
	Error           int                    `json:"error"`
	Hosts           map[string]interface{} `json:"hosts"`
}

func TestRepairAPI(t *testing.T) {
	t.Parallel()

	var createdUnitID uuid.UUID

	basePath := "/api/v1/cluster/" + uuid1.String() + "/repair"

	table := []struct {
		Name           string
		Method         string
		Path           string
		Body           io.Reader
		ExpectedStatus int
		SetupMock      func(*testing.T, *gomock.Controller) *mermaidmock.MockRepairService
		Check          func(*testing.T, *http.Response)
	}{
		{Name: "CreateUnit",
			Method:         http.MethodPost,
			Path:           "/units",
			Body:           strings.NewReader(`{"keyspace": "foo2", "tables": ["table7", "table8"]}`),
			ExpectedStatus: http.StatusCreated,

			SetupMock: func(t *testing.T, ctrl *gomock.Controller) *mermaidmock.MockRepairService {
				svc := mermaidmock.NewMockRepairService(ctrl)
				svc.EXPECT().PutUnit(gomock.Any(), gomock.Any()).Do(func(_ interface{}, u *repair.Unit) {
					if u.ID != uuid.Nil {
						t.Fatal("expected nil uuid")
					}
					if u.ClusterID != uuid1 || u.Keyspace != "foo2" || u.Tables[0] != "table7" || u.Tables[1] != "table8" {
						t.Fatal(u)
					}

					u.ID = uuid.MustRandom()
					createdUnitID = u.ID
				})

				return svc
			},

			Check: func(t *testing.T, resp *http.Response) {
				location := resp.Header.Get("Location")
				prefix := "/api/v1/cluster/" + uuid1.String() + "/repair/unit/"
				if !strings.HasPrefix(location, prefix) {
					t.Fatal("bad location response")
				}
				var unitID uuid.UUID
				if err := unitID.UnmarshalText([]byte(location[len(prefix):])); err != nil {
					t.Fatal("bad unit ID", err)
				}
			},
		},

		{
			Name:           "ListUnits",
			Method:         http.MethodGet,
			Path:           "/units",
			ExpectedStatus: http.StatusOK,

			SetupMock: func(t *testing.T, ctrl *gomock.Controller) *mermaidmock.MockRepairService {
				svc := mermaidmock.NewMockRepairService(ctrl)
				svc.EXPECT().ListUnits(gomock.Any(), uuid1, gomock.Any()).Return(
					[]*repair.Unit{
						{ID: uuid1, ClusterID: uuid1, Keyspace: "keyspace0", Tables: []string{"table1", "table2"}},
						{ID: uuid2, ClusterID: uuid1, Keyspace: "keyspace1", Tables: []string{"table4", "table5"}},
						{ID: createdUnitID, ClusterID: uuid1, Keyspace: "keyspace0", Tables: []string{"table5", "table6"}},
					}, nil)
				return svc
			},

			Check: func(t *testing.T, resp *http.Response) {
				expecting := map[uuid.UUID]int{
					uuid1: 1,
					uuid2: 1,
				}
				expecting[createdUnitID] = 1

				dec := json.NewDecoder(resp.Body)
				result := make([]*repair.Unit, 0, len(expecting))
				if err := dec.Decode(&result); err != nil {
					t.Fatal("json decode failed:", err)
				}

				for _, u := range result {
					if _, ok := expecting[u.ID]; !ok {
						t.Fatal("unexpected result:", u.ID)
					}
					expecting[u.ID]--
				}
				for uuid, count := range expecting {
					if count != 0 {
						t.Fatal("missing result:", uuid)
					}
				}
			},
		},

		{
			Name:           "ListUnits/empty returns empty array",
			Method:         http.MethodGet,
			Path:           "/units",
			ExpectedStatus: http.StatusOK,

			SetupMock: func(t *testing.T, ctrl *gomock.Controller) *mermaidmock.MockRepairService {
				svc := mermaidmock.NewMockRepairService(ctrl)
				svc.EXPECT().ListUnits(gomock.Any(), uuid1, gomock.Any()).Return(nil, nil)
				return svc
			},

			Check: func(t *testing.T, resp *http.Response) {
				var (
					buf    bytes.Buffer
					result []*repair.Unit
				)
				io.Copy(&buf, resp.Body)
				if err := json.NewDecoder(bytes.NewReader(buf.Bytes())).Decode(&result); err != nil {
					t.Fatal("json decode failed:", err)
				}
				if body := strings.TrimSpace(buf.String()); body != "[]" {
					t.Fatal("expected an empty json array, got", body)
				}
			},
		},

		{
			Name:           "GetNonExistantUnit",
			Method:         http.MethodGet,
			Path:           fmt.Sprintf("/unit/%s", uuid3),
			ExpectedStatus: http.StatusNotFound,

			SetupMock: func(t *testing.T, ctrl *gomock.Controller) *mermaidmock.MockRepairService {
				svc := mermaidmock.NewMockRepairService(ctrl)
				svc.EXPECT().GetUnit(gomock.Any(), uuid1, uuid3.String()).Return(nil, mermaid.ErrNotFound)
				return svc
			},
		},

		{
			Name:           "GetExistingUnit",
			Method:         http.MethodGet,
			Path:           fmt.Sprintf("/unit/%s", uuid2),
			ExpectedStatus: http.StatusOK,

			SetupMock: func(t *testing.T, ctrl *gomock.Controller) *mermaidmock.MockRepairService {
				svc := mermaidmock.NewMockRepairService(ctrl)
				svc.EXPECT().GetUnit(gomock.Any(), uuid1, uuid2.String()).Return(
					&repair.Unit{ID: uuid2, ClusterID: uuid1, Keyspace: "keyspace0", Tables: []string{"tbl1", "tbl2"}}, nil)
				return svc
			},

			Check: func(t *testing.T, resp *http.Response) {
				dec := json.NewDecoder(resp.Body)
				var u repair.Unit
				if err := dec.Decode(&u); err != nil {
					t.Fatal("json decode failed:", err)
				}
				if u.ID != uuid2 {
					t.Fatal("unit ID mismatch", uuid2, u)
				}
			},
		},

		{
			Name:           "DeleteUnit",
			Method:         "DELETE",
			Path:           fmt.Sprintf("/unit/%s", uuid2),
			ExpectedStatus: http.StatusOK,

			SetupMock: func(t *testing.T, ctrl *gomock.Controller) *mermaidmock.MockRepairService {
				svc := mermaidmock.NewMockRepairService(ctrl)
				svc.EXPECT().GetUnit(gomock.Any(), uuid1, uuid2.String()).Return(&repair.Unit{ID: uuid2}, nil)
				svc.EXPECT().DeleteUnit(gomock.Any(), uuid1, uuid2).Return(nil)
				return svc
			},
		},

		{
			Name:           "RepairProgress/pre-init",
			Method:         http.MethodGet,
			Path:           fmt.Sprintf("/unit/%s/progress", uuid3),
			ExpectedStatus: http.StatusOK,

			SetupMock: func(t *testing.T, ctrl *gomock.Controller) *mermaidmock.MockRepairService {
				svc := mermaidmock.NewMockRepairService(ctrl)
				tables := []string{"tables1"}

				svc.EXPECT().GetUnit(gomock.Any(), uuid1, uuid3.String()).Return(&repair.Unit{
					ID:        uuid3,
					ClusterID: uuid1,
					Tables:    tables,
				}, nil)
				svc.EXPECT().GetLastRun(gomock.Any(), gomock.Any()).Do(func(_ interface{}, u *repair.Unit) {
					if u.ID != uuid3 {
						t.Fatal("expected", uuid3, "got", u.ID)
					}
				}).Return(
					&repair.Run{ID: uuid2, UnitID: uuid3, ClusterID: uuid1, Keyspace: "test_keyspace", Tables: tables, Status: repair.StatusRunning},
					nil,
				)
				svc.EXPECT().GetProgress(gomock.Any(), gomock.Any(), uuid2).Return([]*repair.RunProgress{
					{ClusterID: uuid1, RunID: uuid2, UnitID: uuid3, Host: "172.16.1.20"},
					{ClusterID: uuid1, RunID: uuid2, UnitID: uuid3, Host: "172.16.1.4"},
					{ClusterID: uuid1, RunID: uuid2, UnitID: uuid3, Host: "172.16.1.5"},
				},
					nil,
				)
				return svc
			},

			Check: func(t *testing.T, resp *http.Response) {
				var prog repairProgress
				prog.PercentComplete = 42
				dec := json.NewDecoder(resp.Body)
				if err := dec.Decode(&prog); err != nil {
					t.Fatal("json decode failed:", err)
				}
				if prog.PercentComplete != 0 {
					t.Fatalf("expected PercentComplete to be 0, %+v\n", prog)
				}
				if len(prog.Hosts) != 3 {
					t.Fatalf("unexpected number of hosts: %+v\n", prog)
				}
			},
		},

		{
			Name:           "RepairProgress/partial-start",
			Method:         http.MethodGet,
			Path:           fmt.Sprintf("/unit/%s/progress", uuid3),
			ExpectedStatus: http.StatusOK,

			SetupMock: func(t *testing.T, ctrl *gomock.Controller) *mermaidmock.MockRepairService {
				svc := mermaidmock.NewMockRepairService(ctrl)
				tables := []string{"tables1"}

				svc.EXPECT().GetUnit(gomock.Any(), uuid1, uuid3.String()).Return(&repair.Unit{
					ID:        uuid3,
					ClusterID: uuid1,
					Tables:    tables,
				}, nil)
				svc.EXPECT().GetLastRun(gomock.Any(), gomock.Any()).Do(func(_ interface{}, u *repair.Unit) {
					if u.ID != uuid3 {
						t.Fatal("expected", uuid3, "got", u.ID)
					}
				}).Return(
					&repair.Run{ID: uuid2, UnitID: uuid3, ClusterID: uuid1, Keyspace: "test_keyspace", Tables: tables, Status: repair.StatusRunning},
					nil,
				)
				svc.EXPECT().GetProgress(gomock.Any(), gomock.Any(), uuid2).Return([]*repair.RunProgress{
					{ClusterID: uuid1, RunID: uuid2, UnitID: uuid3, Host: "172.16.1.20"},
					{ClusterID: uuid1, RunID: uuid2, UnitID: uuid3, Host: "172.16.1.4", Shard: 0, SegmentCount: 1392, SegmentSuccess: 200},
					{ClusterID: uuid1, RunID: uuid2, UnitID: uuid3, Host: "172.16.1.4", Shard: 1, SegmentCount: 1381, SegmentSuccess: 200},
					{ClusterID: uuid1, RunID: uuid2, UnitID: uuid3, Host: "172.16.1.5"},
				},
					nil,
				)
				return svc
			},

			Check: func(t *testing.T, resp *http.Response) {
				var prog repairProgress
				dec := json.NewDecoder(resp.Body)
				if err := dec.Decode(&prog); err != nil {
					t.Fatal("json decode failed:", err)
				}
				if prog.PercentComplete != 4 /* 100 * ((200/1392) + (200/1381) / 2) / 3 */ {
					t.Fatal("expected 4", prog)
				}
			},
		},

		// TODO tests for TaskStop
	}

	logger := log.NewDevelopment()
	for _, test := range table {
		t.Run(test.Name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			clusterStub := mermaidmock.NewMockClusterService(ctrl)
			clusterStub.EXPECT().GetCluster(gomock.Any(), gomock.Any()).Return(&cluster.Cluster{ID: uuid1}, nil)

			h := restapi.New(&restapi.Services{
				Cluster: clusterStub,
				Repair:  test.SetupMock(t, ctrl),
			}, logger)
			r := httptest.NewRequest(test.Method, basePath+test.Path, test.Body)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			resp := w.Result()

			if resp.StatusCode != test.ExpectedStatus {
				t.Fatal("unexpected status code:", resp.StatusCode, "expected:", test.ExpectedStatus)
			}
			if test.Check != nil {
				test.Check(t, resp)
			}
		})
	}
}
