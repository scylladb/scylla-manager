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

	table := []struct {
		Name           string
		Method         string
		Path           string
		ClusterID      uuid.UUID
		Body           io.Reader
		ExpectedStatus int
		SetupMock      func(*testing.T, *gomock.Controller) *mermaidmock.MockRepairService
		Check          func(*testing.T, *http.Response)
	}{
		{Name: "CreateUnit",
			Method: http.MethodPost,
			Path:   "/api/v1/cluster/{cluster_id}/repair/units", ClusterID: uuid1,
			Body:           strings.NewReader(`{"keyspace": "foo2", "tables": ["table7", "table8"]}`),
			ExpectedStatus: http.StatusCreated,

			SetupMock: func(t *testing.T, ctrl *gomock.Controller) *mermaidmock.MockRepairService {
				svc := mermaidmock.NewMockRepairService(ctrl)
				svc.EXPECT().PutUnit(gomock.Any(), gomock.Any()).Do(func(_ interface{}, u *repair.Unit) {
					t.Logf("u: %+v\n", u)
					if u.ID == uuid.Nil {
						t.Fail()
					}
					if u.ClusterID != uuid1 || u.Keyspace != "foo2" || u.Tables[0] != "table7" || u.Tables[1] != "table8" {
						t.Fail()
					}
					createdUnitID = u.ID
				})

				return svc
			},

			Check: func(t *testing.T, resp *http.Response) {
				location := resp.Header.Get("Location")
				prefix := "/api/v1/cluster/" + uuid1.String() + "/repair/unit/"
				if !strings.HasPrefix(location, prefix) {
					t.Log("bad location response")
					t.Fatal()
				}
				var unitID uuid.UUID
				if err := unitID.UnmarshalText([]byte(location[len(prefix):])); err != nil {
					t.Log("bad unit ID", err)
					t.Fatal()
				}
			}},

		{Name: "ListUnits",
			Method: http.MethodGet,
			Path:   "/api/v1/cluster/{cluster_id}/repair/units", ClusterID: uuid1,
			ExpectedStatus: http.StatusOK,

			SetupMock: func(t *testing.T, ctrl *gomock.Controller) *mermaidmock.MockRepairService {
				svc := mermaidmock.NewMockRepairService(ctrl)
				svc.EXPECT().ListUnits(gomock.Any(), uuid1).Return(
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
					t.Log("json decode failed:", err)
					t.Fatal()
				}
				for _, u := range result {
					if _, ok := expecting[u.ID]; !ok {
						t.Log("unexpected result:", u.ID)
						t.Fail()
					}
					expecting[u.ID]--
				}
				for uuid, count := range expecting {
					if count != 0 {
						t.Log("missing result:", uuid)
						t.Fail()
					}
				}
			}},

		{Name: "ListUnits/empty returns empty array",
			Method: http.MethodGet,
			Path:   "/api/v1/cluster/{cluster_id}/repair/units", ClusterID: uuid1,
			ExpectedStatus: http.StatusOK,

			SetupMock: func(t *testing.T, ctrl *gomock.Controller) *mermaidmock.MockRepairService {
				svc := mermaidmock.NewMockRepairService(ctrl)
				svc.EXPECT().ListUnits(gomock.Any(), uuid1).Return(nil, nil)
				return svc
			},

			Check: func(t *testing.T, resp *http.Response) {
				var (
					buf    bytes.Buffer
					result []*repair.Unit
				)
				io.Copy(&buf, resp.Body)
				dec := json.NewDecoder(bytes.NewReader(buf.Bytes()))
				if err := dec.Decode(&result); err != nil {
					t.Log("json decode failed:", err)
					t.Fatal()
				}
				if body := strings.TrimSpace(buf.String()); body != "[]" {
					t.Logf("expected an empty json array, got %q", body)
					t.Fatal()
				}
			}},

		{Name: "GetNonExistantUnit",
			Method: http.MethodGet,
			Path:   "/api/v1/cluster/{cluster_id}/repair/unit/" + uuid3.String(), ClusterID: uuid1,
			ExpectedStatus: http.StatusNotFound,

			SetupMock: func(t *testing.T, ctrl *gomock.Controller) *mermaidmock.MockRepairService {
				svc := mermaidmock.NewMockRepairService(ctrl)
				svc.EXPECT().GetUnit(gomock.Any(), uuid1, uuid3).Return(nil, mermaid.ErrNotFound)
				return svc
			},
		},

		{Name: "GetExistingUnit",
			Method: http.MethodGet,
			Path:   "/api/v1/cluster/{cluster_id}/repair/unit/" + uuid1.String(), ClusterID: uuid2,
			ExpectedStatus: http.StatusOK,

			SetupMock: func(t *testing.T, ctrl *gomock.Controller) *mermaidmock.MockRepairService {
				svc := mermaidmock.NewMockRepairService(ctrl)
				svc.EXPECT().GetUnit(gomock.Any(), uuid2, uuid1).Return(
					&repair.Unit{ID: uuid1, ClusterID: uuid2, Keyspace: "keyspace0", Tables: []string{"tbl1", "tbl2"}}, nil)
				return svc
			},

			Check: func(t *testing.T, resp *http.Response) {
				dec := json.NewDecoder(resp.Body)
				var u repair.Unit
				if err := dec.Decode(&u); err != nil {
					t.Log("json decode failed:", err)
					t.Fatal()
				}
				if u.ID != uuid1 {
					t.Log("unit ID mismatch", uuid1, u)
					t.Fail()
				}
			}},

		{Name: "DeleteUnit",
			Method: "DELETE",
			Path:   "/api/v1/cluster/{cluster_id}/repair/unit/" + uuid2.String(), ClusterID: uuid1,
			ExpectedStatus: http.StatusOK,

			SetupMock: func(t *testing.T, ctrl *gomock.Controller) *mermaidmock.MockRepairService {
				svc := mermaidmock.NewMockRepairService(ctrl)
				svc.EXPECT().DeleteUnit(gomock.Any(), uuid1, uuid2).Return(nil)
				return svc
			},
		},

		{Name: "TaskStats/missing-unit-ID",
			Method:         http.MethodGet,
			Path:           fmt.Sprintf("/api/v1/cluster/{cluster_id}/repair/task/%s", uuid2),
			ClusterID:      uuid1,
			ExpectedStatus: http.StatusBadRequest,

			SetupMock: func(t *testing.T, ctrl *gomock.Controller) *mermaidmock.MockRepairService {
				return mermaidmock.NewMockRepairService(ctrl)
			},
		},

		{Name: "TaskStats/malformed-unit-ID",
			Method:         http.MethodGet,
			Path:           fmt.Sprintf("/api/v1/cluster/{cluster_id}/repair/task/%s?unit_id=abcd", uuid2),
			ClusterID:      uuid1,
			ExpectedStatus: http.StatusBadRequest,

			SetupMock: func(t *testing.T, ctrl *gomock.Controller) *mermaidmock.MockRepairService {
				return mermaidmock.NewMockRepairService(ctrl)
			},
		},

		{Name: "RepairProgress/pre-init",
			Method:         http.MethodGet,
			Path:           fmt.Sprintf("/api/v1/cluster/{cluster_id}/repair/task/%s?unit_id=%s", uuid2, uuid3),
			ClusterID:      uuid1,
			ExpectedStatus: http.StatusOK,

			SetupMock: func(t *testing.T, ctrl *gomock.Controller) *mermaidmock.MockRepairService {
				svc := mermaidmock.NewMockRepairService(ctrl)
				tables := []string{"tables1"}

				svc.EXPECT().GetUnit(gomock.Any(), uuid1, uuid3).Return(&repair.Unit{
					ID:        uuid3,
					ClusterID: uuid1,
					Tables:    tables,
				}, nil)
				svc.EXPECT().GetRun(gomock.Any(), gomock.Any(), uuid2).Return(
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
					t.Log("json decode failed:", err)
					t.Fatal()
				}
				if prog.PercentComplete != 0 {
					t.Logf("expected PercentComplete to be 0, %+v\n", prog)
					t.Fail()
				}
				if len(prog.Hosts) != 3 {
					t.Logf("unexpected number of hosts: %+v\n", prog)
					t.Fail()
				}
			},
		},

		{Name: "RepairProgress/partial-start",
			Method:         http.MethodGet,
			Path:           fmt.Sprintf("/api/v1/cluster/{cluster_id}/repair/task/%s?unit_id=%s", uuid2, uuid3),
			ClusterID:      uuid1,
			ExpectedStatus: http.StatusOK,

			SetupMock: func(t *testing.T, ctrl *gomock.Controller) *mermaidmock.MockRepairService {
				svc := mermaidmock.NewMockRepairService(ctrl)
				tables := []string{"tables1"}

				svc.EXPECT().GetUnit(gomock.Any(), uuid1, uuid3).Return(&repair.Unit{
					ID:        uuid3,
					ClusterID: uuid1,
					Tables:    tables,
				}, nil)
				svc.EXPECT().GetRun(gomock.Any(), gomock.Any(), uuid2).Return(
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
					t.Log("json decode failed:", err)
					t.Fatal()
				}
				if prog.PercentComplete != 4 /* 100 * ((200/1392) + (200/1381) / 2) / 3 */ {
					t.Logf("unexpected PercentComplete: %+v\n", prog)
					t.Fail()
				}
			},
		},

		// TODO tests for TaskStop
	}

	logger := log.NewDevelopment()
	for _, test := range table {
		test := test
		t.Run(test.Name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			h := restapi.New(test.SetupMock(t, ctrl), logger)
			r := httptest.NewRequest(test.Method, strings.Replace(test.Path, "{cluster_id}", test.ClusterID.String(), 1), test.Body)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			resp := w.Result()

			if resp.StatusCode != test.ExpectedStatus {
				t.Log("unexpected status code:", resp.StatusCode, "expected:", test.ExpectedStatus)
				t.Fatal()
			}
			if test.Check != nil {
				test.Check(t, resp)
			}
		})
	}
}
