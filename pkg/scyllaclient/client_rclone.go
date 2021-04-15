// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/rclone/rcserver"
	"github.com/scylladb/scylla-manager/pkg/util/pointer"
	agentClient "github.com/scylladb/scylla-manager/swagger/gen/agent/client"
	"github.com/scylladb/scylla-manager/swagger/gen/agent/client/operations"
	"github.com/scylladb/scylla-manager/swagger/gen/agent/models"
)

// RcloneSetBandwidthLimit sets bandwidth limit of all the current and future
// transfers performed under current client session.
// Limit is expressed in MiB per second.
// To turn off limitation set it to 0.
func (c *Client) RcloneSetBandwidthLimit(ctx context.Context, host string, limit int) error {
	p := operations.CoreBwlimitParams{
		Context:       forceHost(ctx, host),
		BandwidthRate: &models.Bandwidth{Rate: fmt.Sprintf("%dM", limit)},
	}
	_, err := c.agentOps.CoreBwlimit(&p) // nolint: errcheck
	return err
}

// RcloneJobStop stops running job.
func (c *Client) RcloneJobStop(ctx context.Context, host string, jobID int64) error {
	p := operations.JobStopParams{
		Context: forceHost(ctx, host),
		Jobid:   &models.Jobid{Jobid: jobID},
	}
	_, err := c.agentOps.JobStop(&p) // nolint: errcheck
	return err
}

// RcloneJobInfo groups stats for job, running, and completed transfers.
type RcloneJobInfo = models.JobInfo

// RcloneJobProgress aggregates job progress stats.
type RcloneJobProgress = models.JobProgress

// RcloneTransfer represents a single file transfer in RcloneJobProgress Transferred.
type RcloneTransfer = models.Transfer

// GlobalProgressID represents empty job id.
// Use this value to return global stats by job info.
var GlobalProgressID int64 = 0

// RcloneJobInfo returns job stats, and transfers info about running stats and
// completed transfers.
// If waitSeconds > 0 then long polling will be used with number of seconds.
func (c *Client) RcloneJobInfo(ctx context.Context, host string, jobID int64, waitSeconds int) (*RcloneJobInfo, error) {
	ctx = customTimeout(forceHost(ctx, host), c.longPollingTimeout(waitSeconds))

	p := operations.JobInfoParams{
		Context: ctx,
		Jobinfo: &models.JobInfoParams{
			Jobid: jobID,
			Wait:  int64(waitSeconds),
		},
	}
	resp, err := c.agentOps.JobInfo(&p)
	if err != nil {
		return nil, err
	}

	return resp.Payload, nil
}

// RcloneJobProgress returns aggregated stats for the job along with its status.
func (c *Client) RcloneJobProgress(ctx context.Context, host string, jobID int64, waitSeconds int) (*RcloneJobProgress, error) {
	ctx = customTimeout(forceHost(ctx, host), c.longPollingTimeout(waitSeconds))

	p := operations.JobProgressParams{
		Context: ctx,
		Jobinfo: &models.JobInfoParams{
			Jobid: jobID,
			Wait:  int64(waitSeconds),
		},
	}
	resp, err := c.agentOps.JobProgress(&p)
	if StatusCodeOf(err) == http.StatusNotFound {
		// If we got 404 then return empty progress with not found status.
		return &RcloneJobProgress{
			Status: string(rcserver.JobNotFound),
		}, nil
	}
	if err != nil {
		return nil, err
	}

	return resp.Payload, nil
}

// RcloneJobStatus returns status of the job.
// There is one running state and three completed states error, not_found, and
// success.
type RcloneJobStatus string

const (
	// JobError signals that job completed with error.
	JobError RcloneJobStatus = "error"
	// JobSuccess signals that job completed with success.
	JobSuccess RcloneJobStatus = "success"
	// JobRunning signals that job is still running.
	JobRunning RcloneJobStatus = "running"
	// JobNotFound signals that job is no longer available.
	JobNotFound RcloneJobStatus = "not_found"
)

// RcloneDeleteJobStats deletes job stats group.
func (c *Client) RcloneDeleteJobStats(ctx context.Context, host string, jobID int64) error {
	p := operations.CoreStatsDeleteParams{
		Context: forceHost(ctx, host),
		StatsParams: &models.StatsParams{
			Group: rcloneDefaultGroup(jobID),
		},
	}
	_, err := c.agentOps.CoreStatsDelete(&p) // nolint: errcheck
	return err
}

// RcloneResetStats resets stats.
func (c *Client) RcloneResetStats(ctx context.Context, host string) error {
	p := operations.CoreStatsResetParams{
		Context: forceHost(ctx, host),
	}
	_, err := c.agentOps.CoreStatsReset(&p) // nolint: errcheck
	return err
}

// RcloneDefaultGroup returns default group name based on job id.
func rcloneDefaultGroup(jobID int64) string {
	return fmt.Sprintf("job/%d", jobID)
}

// RcloneMoveFile moves file from srcRemotePath to dstRemotePath.
// Remotes need to be registered with the server first.
// Remote path format is "name:bucket/path".
// Both dstRemotePath and srRemotePath must point to a file.
func (c *Client) RcloneMoveFile(ctx context.Context, host, dstRemotePath, srcRemotePath string) error {
	dstFs, dstRemote, err := rcloneSplitRemotePath(dstRemotePath)
	if err != nil {
		return err
	}
	srcFs, srcRemote, err := rcloneSplitRemotePath(srcRemotePath)
	if err != nil {
		return err
	}
	p := operations.OperationsMovefileParams{
		Context: forceHost(ctx, host),
		Options: &models.MoveOrCopyFileOptions{
			DstFs:     dstFs,
			DstRemote: dstRemote,
			SrcFs:     srcFs,
			SrcRemote: srcRemote,
		},
	}
	_, err = c.agentOps.OperationsMovefile(&p)
	return err
}

// RcloneCopyDir copies contents of the directory pointed by srcRemotePath to
// the directory pointed by dstRemotePath.
// Remotes need to be registered with the server first.
// Returns ID of the asynchronous job.
// Remote path format is "name:bucket/path".
func (c *Client) RcloneCopyDir(ctx context.Context, host, dstRemotePath, srcRemotePath string) (int64, error) {
	dstFs, dstRemote, err := rcloneSplitRemotePath(dstRemotePath)
	if err != nil {
		return 0, err
	}
	srcFs, srcRemote, err := rcloneSplitRemotePath(srcRemotePath)
	if err != nil {
		return 0, err
	}
	c.logger.Debug(ctx, "RcloneCopyDir", "DstFs", dstFs, "DstRemote", dstRemote, "SrcFs", srcFs, "SrcRemote", srcRemote)
	p := operations.SyncCopyDirParams{
		Context: forceHost(ctx, host),
		Options: &models.MoveOrCopyFileOptions{
			DstFs:     dstFs,
			DstRemote: dstRemote,
			SrcFs:     srcFs,
			SrcRemote: srcRemote,
		},
		Async: true,
	}
	resp, err := c.agentOps.SyncCopyDir(&p)
	if err != nil {
		return 0, err
	}
	return resp.Payload.Jobid, nil
}

// RcloneDeleteDir removes a directory or container and all of its contents
// from the remote.
// Remote path format is "name:bucket/path".
func (c *Client) RcloneDeleteDir(ctx context.Context, host, remotePath string) error {
	fs, remote, err := rcloneSplitRemotePath(remotePath)
	if err != nil {
		return err
	}
	p := operations.OperationsPurgeParams{
		Context: forceHost(ctx, host),
		RemotePath: &models.RemotePath{
			Fs:     fs,
			Remote: remote,
		},
		Async: false,
	}
	_, err = c.agentOps.OperationsPurge(&p) // nolint: errcheck
	return err
}

// RcloneDeleteFile removes the single file pointed to by remotePath
// Remote path format is "name:bucket/path".
func (c *Client) RcloneDeleteFile(ctx context.Context, host, remotePath string) error {
	fs, remote, err := rcloneSplitRemotePath(remotePath)
	if err != nil {
		return err
	}
	p := operations.OperationsDeletefileParams{
		Context: forceHost(ctx, host),
		RemotePath: &models.RemotePath{
			Fs:     fs,
			Remote: remote,
		},
		Async: false,
	}
	_, err = c.agentOps.OperationsDeletefile(&p) // nolint: errcheck
	return err
}

// RcloneDiskUsage get disk space usage.
// Remote path format is "name:bucket/path".
func (c *Client) RcloneDiskUsage(ctx context.Context, host, remotePath string) (*models.FileSystemDetails, error) {
	p := operations.OperationsAboutParams{
		Context: forceHost(ctx, host),
		RemotePath: &models.RemotePath{
			Fs: remotePath,
		},
	}
	resp, err := c.agentOps.OperationsAbout(&p)
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

// RcloneOpen streams remote file content. The stream is an HTTP body.
// Callers must close the body after use.
func (c *Client) RcloneOpen(ctx context.Context, host, remotePath string) (io.ReadCloser, error) {
	fs, remote, err := rcloneSplitRemotePath(remotePath)
	if err != nil {
		return nil, err
	}

	// Due to missing generator for Swagger 3.0, and poor implementation of 2.0
	// raw file download we are downloading manually.
	const urlPath = agentClient.DefaultBasePath + "/rclone/operations/cat"

	// Body
	b, err := (&models.RemotePath{
		Fs:     fs,
		Remote: remote,
	}).MarshalBinary()
	if err != nil {
		return nil, err
	}

	u := c.newURL(host, urlPath)
	req, err := http.NewRequestWithContext(forceHost(ctx, host), http.MethodPost, u.String(), bytes.NewBuffer(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.transport.RoundTrip(req)
	if err != nil {
		return nil, errors.Wrap(err, "round trip")
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		return nil, makeAgentError(resp)
	}

	return resp.Body, nil
}

// RcloneCat returns a content of a remote path.
func (c *Client) RcloneCat(ctx context.Context, host, remotePath string) ([]byte, error) {
	r, err := c.RcloneOpen(ctx, host, remotePath)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return ioutil.ReadAll(r)
}

// RcloneListDirOpts specifies options for RcloneListDir.
type RcloneListDirOpts = models.ListOptionsOpt

// RcloneListDirItem represents a file in a listing with RcloneListDir.
type RcloneListDirItem = models.ListItem

// RcloneListDir returns contents of a directory specified by remotePath.
// The remotePath is given in the following format "provider:bucket/path".
// Resulting item path is relative to the remote path.
//
// LISTING REMOTE DIRECTORIES IS KNOWN TO BE EXTREMELY SLOW.
// DO NOT USE THIS FUNCTION FOR ANYTHING BUT A FLAT DIRECTORY LISTING.
// FOR OTHER NEEDS, OR WHEN THE NUMBER OF FILES CAN BE BIG, USE RcloneListDirIter.
//
// This function must execute in the standard Timeout (15s by default) and
// will be retried if failed.
func (c *Client) RcloneListDir(ctx context.Context, host, remotePath string, opts *RcloneListDirOpts) ([]*RcloneListDirItem, error) {
	p := operations.OperationsListParams{
		Context: forceHost(ctx, host),
		ListOpts: &models.ListOptions{
			Fs:     &remotePath,
			Remote: pointer.StringPtr(""),
			Opt:    opts,
		},
	}
	resp, err := c.agentOps.OperationsList(&p)
	if err != nil {
		return nil, err
	}

	return resp.Payload.List, nil
}

// RcloneListDirIter returns contents of a directory specified by remotePath.
// The remotePath is given in the following format "provider:bucket/path".
// Resulting item path is relative to the remote path.
func (c *Client) RcloneListDirIter(ctx context.Context, host, remotePath string, opts *RcloneListDirOpts, f func(item *RcloneListDirItem)) error {
	ctx = customTimeout(ctx, c.config.ListTimeout)

	// Due to OpenAPI limitations we manually construct and sent the request
	// object to stream process the response body.
	const urlPath = agentClient.DefaultBasePath + "/rclone/operations/list"

	listOpts := &models.ListOptions{
		Fs:     &remotePath,
		Remote: pointer.StringPtr(""),
		Opt:    opts,
	}
	b, err := listOpts.MarshalBinary()
	if err != nil {
		return err
	}

	u := c.newURL(host, urlPath)
	req, err := http.NewRequestWithContext(forceHost(ctx, host), http.MethodPost, u.String(), bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")

	resp, err := c.transport.RoundTrip(req)
	if err != nil {
		return errors.Wrap(err, "round trip")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return makeAgentError(resp)
	}

	dec := json.NewDecoder(resp.Body)

	// Skip tokens down to array opening
	expected := []string{"{", "list", "["}
	for i := range expected {
		tok, err := dec.Token()
		if err != nil {
			return errors.Wrap(err, "read token")
		}
		if fmt.Sprint(tok) != expected[i] {
			return errors.Errorf("json unexpected token %s expected %s", tok, expected[i])
		}
	}

	var v RcloneListDirItem
	for dec.More() {
		// Detect context cancellation
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Read value
		v = RcloneListDirItem{}
		if err := dec.Decode(&v); err != nil {
			return err
		}
		f(&v)
	}

	return nil
}

// RcloneCheckPermissions checks if location is available for listing, getting,
// creating, and deleting objects.
func (c *Client) RcloneCheckPermissions(ctx context.Context, host, remotePath string) error {
	p := operations.OperationsCheckPermissionsParams{
		Context: forceHost(ctx, host),
		RemotePath: &models.RemotePath{
			Fs:     remotePath,
			Remote: "",
		},
	}
	_, err := c.agentOps.OperationsCheckPermissions(&p)
	return err
}

// RclonePut uploads file with provided content under remotePath.
func (c *Client) RclonePut(ctx context.Context, host, remotePath string, content io.Reader, size int64) error {
	fs, remote, err := rcloneSplitRemotePath(remotePath)
	if err != nil {
		return err
	}

	// Due to missing generator for Swagger 3.0, and poor implementation of 2.0 file upload
	// we are uploading manually.
	const urlPath = agentClient.DefaultBasePath + "/rclone/operations/put"

	u := c.newURL(host, urlPath)
	req, err := http.NewRequestWithContext(forceHost(ctx, host), http.MethodPost, u.String(), content)
	if err != nil {
		return err
	}

	q := req.URL.Query()
	q.Add("fs", fs)
	q.Add("remote", remote)
	req.URL.RawQuery = q.Encode()

	req.Header.Add("Content-Type", "application/octet-stream")
	req.Header.Add("Content-Length", fmt.Sprint(size))

	resp, err := c.transport.RoundTrip(req)
	if err != nil {
		return errors.Wrap(err, "round trip")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return makeAgentError(resp)
	}

	return nil
}

// rcloneSplitRemotePath splits string path into file system and file path.
func rcloneSplitRemotePath(remotePath string) (fs, path string, err error) {
	parts := strings.Split(remotePath, ":")
	if len(parts) != 2 {
		err = errors.New("remote path without file system name")
		return
	}

	dirParts := strings.SplitN(parts[1], "/", 2)
	root := dirParts[0]
	fs = fmt.Sprintf("%s:%s", parts[0], root)
	if len(dirParts) > 1 {
		path = dirParts[1]
	}
	return
}
