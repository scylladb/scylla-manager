// Copyright (C) 2026 ScyllaDB

//go:build all || integration

package backup_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-openapi/strfmt"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/v3/pkg/service/backup"
	"github.com/scylladb/scylla-manager/v3/pkg/sstable"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/db"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util/httpx"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
	"github.com/scylladb/scylla-manager/v3/pkg/util/uuid"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/agent/models"
)

const (
	eventBasedHoldPath = "/agent/rclone/operations/event-based-hold"
	fileInfoPath       = "/agent/rclone/operations/fileinfo"
	listPath           = "/agent/rclone/operations/list"
)

// Our GCS mock doesn't have CRUD support for event based hold.
// It simply ignores this param and does not return it.
// To allow testing, we use a simple custom interceptor which:
// - manages in memory event based hold state for all known objects based on
// default event based hold state and registered per object requests modifying it.
// - enriches responses with event based hold state.
// It supports event based hold and bucket retention policy interactions in terms
// of tracking them, but does not enforce them.
type eventBasedHoldInterceptor struct {
	t *testing.T

	defaultBucketRetentionPolicy time.Duration
	defaultEventBasedHold        bool
	now                          func() time.Time

	mu    sync.Mutex
	state map[string]eventBasedHoldState
}

type eventBasedHoldState struct {
	eventBasedHold bool
	retainUntil    time.Time
}

func newEventBasedHoldInterceptor(t *testing.T, hrt *HackableRoundTripper) *eventBasedHoldInterceptor {
	i := &eventBasedHoldInterceptor{
		t:     t,
		now:   time.Now,
		state: make(map[string]eventBasedHoldState),
	}
	hrt.SetInterceptor(httpx.RoundTripperFunc(i.interceptReq))
	hrt.SetRespInterceptor(i.interceptResp)
	return i
}

func (i *eventBasedHoldInterceptor) defaultState() eventBasedHoldState {
	if i.defaultEventBasedHold {
		return eventBasedHoldState{eventBasedHold: i.defaultEventBasedHold}
	}
	return eventBasedHoldState{retainUntil: i.now().Add(i.defaultBucketRetentionPolicy)}
}

func (i *eventBasedHoldInterceptor) getState(key string) eventBasedHoldState {
	i.mu.Lock()
	defer i.mu.Unlock()

	if st, ok := i.state[key]; ok {
		return st
	}
	st := i.defaultState()
	i.state[key] = st
	return st
}

func (i *eventBasedHoldInterceptor) setState(key string, hold bool) {
	st := i.getState(key)
	// Event based hold removal triggers bucket retention policy
	if st.eventBasedHold && !hold {
		st.retainUntil = i.now().Add(i.defaultBucketRetentionPolicy)
	}
	// Event based hold application resets bucket retention policy
	if hold {
		st.retainUntil = time.Time{}
	}
	st.eventBasedHold = hold
	i.mu.Lock()
	i.state[key] = st
	i.mu.Unlock()
}

func (i *eventBasedHoldInterceptor) resolveState(key string, mockRetainUntil time.Time, mockMode string) (hold bool, retainUntil time.Time, mode string) {
	st := i.getState(key)
	if st.eventBasedHold {
		return true, time.Time{}, ""
	} else if st.retainUntil.After(mockRetainUntil) {
		return false, st.retainUntil, "locked"
	}
	return false, mockRetainUntil, mockMode
}

func (i *eventBasedHoldInterceptor) interceptReq(req *http.Request) (*http.Response, error) {
	switch req.URL.Path {
	case eventBasedHoldPath:
		var opts models.EventBasedHoldOptions
		decodeReqBody(i.t, req, &opts)
		for _, p := range opts.Paths {
			key := path.Join(opts.Fs, opts.Remote, p)
			i.setState(key, opts.EventBasedHold)
		}
		return nil, nil
	case fileInfoPath, listPath:
		makeReqBodyReplayable(i.t, req)
		return nil, nil
	default:
		return nil, nil
	}
}

func (i *eventBasedHoldInterceptor) interceptResp(resp *http.Response, err error) (*http.Response, error) {
	if err != nil || resp == nil || resp.Request == nil {
		return nil, nil
	}

	switch resp.Request.URL.Path {
	case fileInfoPath:
		var remotePath models.RemotePath
		decodeReqBody(i.t, resp.Request, &remotePath)
		var info models.FileInfo
		decodeBody(i.t, resp.Body, &info)

		key := path.Join(remotePath.Fs, remotePath.Remote)
		hold, retainUntil, mode := i.resolveState(key, time.Time(info.RetainUntil), info.RetentionMode)
		info.EventBasedHold = hold
		info.RetainUntil = strfmt.DateTime(retainUntil)
		info.RetentionMode = mode
		return encodeRespBody(i.t, resp, &info), nil
	case listPath:
		var opts models.ListOptions
		decodeReqBody(i.t, resp.Request, &opts)
		if opts.Opt != nil && !opts.Opt.ShowEventBasedHold {
			return nil, nil
		}

		type listResponse struct {
			List []*models.ListItem `json:"list"`
		}
		var out listResponse
		decodeBody(i.t, resp.Body, &out)

		for _, item := range out.List {
			if item.IsDir {
				continue
			}
			key := path.Join(*opts.Fs, *opts.Remote, item.Path)
			hold, retainUntil, mode := i.resolveState(key, time.Time(item.RetainUntil), item.RetentionMode)
			item.EventBasedHold = hold
			item.RetainUntil = strfmt.DateTime(retainUntil)
			item.RetentionMode = mode
		}
		return encodeRespBody(i.t, resp, &out), nil
	default:
		return nil, nil
	}
}

func makeReqBodyReplayable(t *testing.T, req *http.Request) {
	if req.GetBody != nil || req.Body == nil {
		return
	}
	rawBody, err := io.ReadAll(req.Body)
	if errs := errors.Join(err, req.Body.Close()); errs != nil {
		t.Error(errs)
		return
	}
	req.Body = io.NopCloser(bytes.NewReader(rawBody))
	req.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(rawBody)), nil
	}
}

func decodeReqBody(t *testing.T, req *http.Request, v any) {
	makeReqBodyReplayable(t, req)
	if req.GetBody == nil {
		t.Error("request body is not replayable")
		return
	}
	body, err := req.GetBody()
	if err != nil {
		t.Error(err)
		return
	}
	decodeBody(t, body, v)
}

func decodeBody(t *testing.T, body io.ReadCloser, v any) {
	rawBody, err := io.ReadAll(body)
	if errs := errors.Join(err, body.Close()); errs != nil {
		t.Error(errs)
		return
	}
	if err := json.Unmarshal(rawBody, v); err != nil {
		t.Error(err)
	}
}

func encodeRespBody(t *testing.T, resp *http.Response, v any) *http.Response {
	body, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body = io.NopCloser(bytes.NewReader(body))
	resp.ContentLength = int64(len(body))
	resp.Header.Set("Content-Length", strconv.Itoa(len(body)))
	return resp
}

func TestBackupEventBasedHoldInterceptorIntegration(t *testing.T) {
	// This test validates scyllaclient event based holds methods and
	// their interaction with event based hold test interceptor.
	const (
		testBucket = "backuptest-event-based-hold-interceptor"
		file       = "file.txt"
	)
	host := ManagedClusterHost()
	policy := 24 * time.Hour
	baseTime := timeutc.Now().Truncate(time.Millisecond)

	location := backupspec.Location{Provider: backupspec.GCS, Path: testBucket}
	GCSInitBucket(t, testBucket)
	h := newBackupTestHelper(t, CreateSessionWithoutMigration(t), defaultConfig(), location, nil)
	remoteFile := location.RemotePath(path.Join("event-based-hold", file))

	Print("Given: bucket with default retention policy and event based hold")
	interceptor := newEventBasedHoldInterceptor(t, h.Hrt)
	interceptor.defaultBucketRetentionPolicy = policy
	interceptor.defaultEventBasedHold = true
	interceptor.now = func() time.Time { return baseTime }

	Print("When: file is uploaded")
	if err := h.Client.RclonePut(t.Context(), host, remoteFile, bytes.NewBufferString("data")); err != nil {
		t.Fatal(err)
	}

	Print("Then: file has event based hold")
	assertObjectMetadata(t, h.Client, host, remoteFile, true, "", time.Time{})

	Print("When: event based hold is removed")
	if err := h.Client.RcloneEventBasedHold(t.Context(), host, remoteFile, false); err != nil {
		t.Fatal(err)
	}
	// Just to verify that the interceptor uses retention policy present at file upload
	interceptor.now = func() time.Time { return baseTime.Add(10 * policy) }

	Print("Then: file has retention lock and no event based hold")
	assertObjectMetadata(t, h.Client, host, remoteFile, false, string(scyllaclient.RetentionModeLocked), baseTime.Add(policy))

	Print("When: event based hold is re-applied")
	if err := h.Client.RcloneEventBasedHold(t.Context(), host, remoteFile, true); err != nil {
		t.Fatal(err)
	}

	Print("Then: file has event based hold and no retention lock")
	assertObjectMetadata(t, h.Client, host, remoteFile, true, "", time.Time{})
}

func TestBackupRetentionLockCRUDIntegration(t *testing.T) {
	// This test verifies that:
	// - new file doesn't have retention lock
	// - such file can have retention lock in unlocked mode applied
	// - such file can have retention lock strengthened to locked mode
	// It does not verify that file with unlocked retention can have its
	// retention cleaned up, as this behavior is lacking on our gcs mock side.
	const (
		testBucket = "backuptest-retention-lock-crud"
		file       = "file.txt"
	)
	host := ManagedClusterHost()

	location := backupspec.Location{Provider: backupspec.GCS, Path: testBucket}
	GCSInitBucket(t, testBucket)
	h := newBackupTestHelper(t, CreateSessionWithoutMigration(t), defaultConfig(), location, nil)
	remoteFile := location.RemotePath(path.Join("retention-lock", file))

	Print("When: file is uploaded")
	if err := h.Client.RclonePut(t.Context(), host, remoteFile, bytes.NewBufferString("data")); err != nil {
		t.Fatal(err)
	}

	Print("Then: file has no retention lock")
	assertObjectMetadata(t, h.Client, host, remoteFile, false, "", time.Time{})

	Print("When: unlocked retention is applied")
	until := timeutc.Now().Add(24 * time.Hour).Truncate(time.Second)
	if err := h.Client.RcloneRetentionLock(t.Context(), host, remoteFile, scyllaclient.RetentionModeUnlocked, until, false); err != nil {
		t.Fatal(err)
	}

	Print("Then: file has unlocked retention")
	assertObjectMetadata(t, h.Client, host, remoteFile, false, string(scyllaclient.RetentionModeUnlocked), until)

	Print("When: retention is upgraded to locked")
	if err := h.Client.RcloneRetentionLock(t.Context(), host, remoteFile, scyllaclient.RetentionModeLocked, until, true); err != nil {
		t.Fatal(err)
	}

	Print("Then: file has locked retention")
	assertObjectMetadata(t, h.Client, host, remoteFile, false, string(scyllaclient.RetentionModeLocked), until)
}

func TestBackupEventBasedHoldIntegration(t *testing.T) {
	// This test verifies basic event based hold backup flow:
	// - newly uploaded files (manifests, schema files, sstables) have hold
	// - deduplicated files have hold
	// - files from non-current snapshot don't have hold
	// - files with hold manually changed are adjusted
	const (
		testBucket   = "backuptest-event-based-hold"
		testKeyspace = "backuptest_event_based_hold"
	)
	host := ManagedClusterHost()
	policy := 24 * time.Hour
	// To avoid precision error on round trip comparisons
	baseTime := timeutc.Now().Truncate(time.Millisecond)

	location := backupspec.Location{Provider: backupspec.GCS, Path: testBucket}
	GCSInitBucket(t, testBucket)
	h := newBackupTestHelper(t, CreateSessionWithoutMigration(t), defaultConfig(), location, nil)
	clusterSession := CreateSessionAndDropAllKeyspaces(t, h.Client)

	interceptor := newEventBasedHoldInterceptor(t, h.Hrt)
	interceptor.defaultBucketRetentionPolicy = policy
	interceptor.defaultEventBasedHold = true
	interceptor.now = func() time.Time { return baseTime }

	nextID := RawWriteData(t, clusterSession, testKeyspace, 0, 1, "{'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}", false)

	Print("Given: backup target with event based hold")
	props := defaultTestProperties(location, testKeyspace)
	props["dc"] = []string{"dc1"}
	props["retention"] = 0
	props["retention_days"] = 2
	props["method"] = backup.MethodAuto
	rawProps, err := json.Marshal(props)
	if err != nil {
		t.Fatal(err)
	}
	target, err := h.service.GetTarget(t.Context(), h.ClusterID, rawProps)
	if err != nil {
		t.Fatal(err)
	}
	target.RetentionLockMode = backup.RetentionLockEventBasedHold
	// To skip not interesting schema sstables
	target.Units = []backup.Unit{{Keyspace: testKeyspace}}

	Print("When: first backup is executed")
	runID := uuid.NewTime()
	if err := h.service.Backup(t.Context(), h.ClusterID, h.TaskID, runID, target); err != nil {
		t.Fatal(err)
	}
	tagA, err := h.service.GetProgress(t.Context(), h.ClusterID, h.TaskID, runID)
	if err != nil {
		t.Fatal(err)
	}

	Print("Then: all files from the snapshot have event based hold")
	tagAFiles := listSnapshotFiles(t, h, tagA.SnapshotTag)
	assertObjectMetadataAll(t, h.Client, host, location, tagAFiles, true, "", time.Time{})

	tagATOCFiles := make([]string, 0)
	for _, file := range tagAFiles {
		if strings.HasSuffix(file, string(sstable.ComponentTOC)) {
			tagATOCFiles = append(tagATOCFiles, file)
		}
	}
	if len(tagATOCFiles) == 0 {
		t.Fatal("Expected TOC components")
	}

	// Clearing hold only from TOC components creates inconsistent remote sstables,
	// as all sstable components should have the same hold state.
	// Deduplicated sstables need hold re-applied on TOC components,
	// while non-current sstables need hold removed from the remaining components.
	Print("When: event based hold is manually removed from every TOC component")
	for _, file := range tagATOCFiles {
		if err := h.Client.RcloneEventBasedHold(t.Context(), host, location.RemotePath(file), false); err != nil {
			t.Fatal(err)
		}
	}
	assertObjectMetadataAll(t, h.Client, host, location, tagATOCFiles, false, string(scyllaclient.RetentionModeLocked), baseTime.Add(policy))

	// The second backup should contain both deduplicated and new sstables
	Print("And: small amount of new data is inserted")
	RawWriteData(t, clusterSession, testKeyspace, nextID, 1, "{'class': 'NetworkTopologyStrategy', 'dc1': 3, 'dc2': 3}", false)

	Print("And: second backup is executed")
	runID = uuid.NewTime()
	if err := h.service.Backup(t.Context(), h.ClusterID, h.TaskID, runID, target); err != nil {
		t.Fatal(err)
	}
	tagB, err := h.service.GetProgress(t.Context(), h.ClusterID, h.TaskID, runID)
	if err != nil {
		t.Fatal(err)
	}

	Print("Then: all files from the current snapshot have event based hold")
	tagBFiles := listSnapshotFiles(t, h, tagB.SnapshotTag)
	assertObjectMetadataAll(t, h.Client, host, location, tagBFiles, true, "", time.Time{})

	tagBFilesSet := make(map[string]struct{}, len(tagBFiles))
	for _, file := range tagBFiles {
		tagBFilesSet[file] = struct{}{}
	}
	var tagAOnlyFiles []string
	for _, file := range tagAFiles {
		if _, ok := tagBFilesSet[file]; !ok {
			tagAOnlyFiles = append(tagAOnlyFiles, file)
		}
	}
	if len(tagAOnlyFiles) == 0 {
		t.Fatal("Expected some files to be present only in the first snapshot")
	}

	Print("And: files from non-current snapshots don't have event based hold")
	assertObjectMetadataAll(t, h.Client, host, location, tagAOnlyFiles, false, string(scyllaclient.RetentionModeLocked), baseTime.Add(policy))
}

func assertObjectMetadataAll(t *testing.T, client *scyllaclient.Client, host string, location backupspec.Location, files []string, hold bool, retentionMode string, retainUntil time.Time) {
	for _, file := range files {
		assertObjectMetadata(t, client, host, location.RemotePath(file), hold, retentionMode, retainUntil)
	}
}

func assertObjectMetadata(t *testing.T, client *scyllaclient.Client, host, objectPath string, hold bool, retentionMode string, retainUntil time.Time) {
	remoteDir, _ := path.Split(objectPath)
	key := path.Join(objectPath)

	info, err := client.RcloneFileInfo(t.Context(), host, objectPath)
	if err != nil {
		t.Fatal(err)
	}
	assertObjectFields(t, info.EventBasedHold, info.RetentionMode, time.Time(info.RetainUntil), hold, retentionMode, retainUntil)

	item := listDir(t, client, host, remoteDir, key, &scyllaclient.RcloneListDirOpts{ShowEventBasedHold: true, ShowRetentionInfo: true})
	assertObjectFields(t, item.EventBasedHold, item.RetentionMode, time.Time(item.RetainUntil), hold, retentionMode, retainUntil)
}

func listDir(t *testing.T, client *scyllaclient.Client, host, remoteDir, key string, opts *scyllaclient.RcloneListDirOpts) *scyllaclient.RcloneListDirItem {
	items, err := client.RcloneListDir(t.Context(), host, remoteDir, opts)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		if path.Join(remoteDir, item.Path) == key {
			return item
		}
	}
	t.Fatalf("List item %q not found in %+v", key, items)
	return nil
}

func assertObjectFields(t *testing.T, gotHold bool, gotMode string, gotUntil time.Time, expectedHold bool, expectedMode string, expectedUntil time.Time) {
	if gotHold != expectedHold {
		t.Fatalf("Expected event based hold %v, got %v", expectedHold, gotHold)
	}
	if gotMode != expectedMode {
		t.Fatalf("Expected retention mode %q, got %q", expectedMode, gotMode)
	}
	if !gotUntil.Equal(expectedUntil) {
		t.Fatalf("Expected retain until %v, got %v", expectedUntil, gotUntil)
	}
}
