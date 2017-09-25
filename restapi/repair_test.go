// Copyright (C) 2017 ScyllaDB

package restapi_test

import (
	"bytes"
	"encoding/json"
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

func TestRepairUnitAPI(t *testing.T) {
	var createdUnitID uuid.UUID
	tests := []struct {
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
			Method: "POST", Path: "/api/v1/cluster/{cluster_id}/repair/units", ClusterID: uuid1,
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
			Method: "GET", Path: "/api/v1/cluster/{cluster_id}/repair/units", ClusterID: uuid1,
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
			Method: "GET", Path: "/api/v1/cluster/{cluster_id}/repair/units", ClusterID: uuid1,
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
			Method: "GET", Path: "/api/v1/cluster/{cluster_id}/repair/unit/" + uuid3.String(), ClusterID: uuid1,
			ExpectedStatus: http.StatusNotFound,
			SetupMock: func(t *testing.T, ctrl *gomock.Controller) *mermaidmock.MockRepairService {
				svc := mermaidmock.NewMockRepairService(ctrl)
				svc.EXPECT().GetUnit(gomock.Any(), uuid1, uuid3).Return(nil, mermaid.ErrNotFound)
				return svc
			},
		},

		{Name: "GetExistingUnit",
			Method: "GET", Path: "/api/v1/cluster/{cluster_id}/repair/unit/" + uuid1.String(), ClusterID: uuid2,
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
			Method: "DELETE", Path: "/api/v1/cluster/{cluster_id}/repair/unit/" + uuid2.String(), ClusterID: uuid1,
			ExpectedStatus: http.StatusOK,
			SetupMock: func(t *testing.T, ctrl *gomock.Controller) *mermaidmock.MockRepairService {
				svc := mermaidmock.NewMockRepairService(ctrl)
				svc.EXPECT().DeleteUnit(gomock.Any(), uuid1, uuid2).Return(nil)
				return svc
			},
		},
	}

	logger := log.NewDevelopment()
	for _, tc := range tests {
		tc := tc
		t.Run(tc.Name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			h := restapi.New(tc.SetupMock(t, ctrl), logger)
			req := httptest.NewRequest(tc.Method, strings.Replace(tc.Path, "{cluster_id}", tc.ClusterID.String(), 1), tc.Body)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			resp := w.Result()

			if resp.StatusCode != tc.ExpectedStatus {
				t.Log("unexpected status code:", resp.StatusCode, "expected:", tc.ExpectedStatus)
				t.Fatal()
			}
			if tc.Check != nil {
				tc.Check(t, resp)
			}
		})
	}
}
