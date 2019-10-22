// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/mermaid/pkg/scyllaclient/internal/agent/client/operations"
	"github.com/scylladb/mermaid/pkg/scyllaclient/internal/agent/models"
	"github.com/scylladb/mermaid/pkg/util/httpmw"
)

// RcloneSetBandwidthLimit sets bandwidth limit of all the current and future
// transfers performed under current client session.
// Limit is expressed in MiB per second.
// To turn off limitation set it to 0.
func (c *Client) RcloneSetBandwidthLimit(ctx context.Context, host string, limit int) error {
	p := operations.CoreBwlimitParams{
		Context:       httpmw.ForceHost(ctx, host),
		BandwidthRate: &models.Bandwidth{Rate: fmt.Sprintf("%dM", limit)},
	}
	_, err := c.agentOps.CoreBwlimit(&p) //nolint:errcheck
	return err
}

// RcloneJobStop stops running job.
func (c *Client) RcloneJobStop(ctx context.Context, host string, jobID int64) error {
	p := operations.JobStopParams{
		Context: httpmw.ForceHost(ctx, host),
		Jobid:   &models.Jobid{Jobid: jobID},
	}
	_, err := c.agentOps.JobStop(&p) //nolint:errcheck
	return err
}

// RcloneJobInfo aggregates current, transferred and job statuses.
type RcloneJobInfo = models.JobInfo

// RcloneJobInfo returns aggregated stats for the job along with job status.
func (c *Client) RcloneJobInfo(ctx context.Context, host string, jobID int64) (*RcloneJobInfo, error) {
	p := operations.JobInfoParams{
		Context: httpmw.ForceHost(ctx, host),
		Jobid:   &models.Jobid{Jobid: jobID},
	}
	resp, err := c.agentOps.JobInfo(&p)
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

// RcloneJobStatus returns status of the job.
// There is one running state and three completed states error, not_found, and
// success.
type RcloneJobStatus string

// RcloneJobStatus enumeration.
const (
	JobError    RcloneJobStatus = "error"
	JobSuccess  RcloneJobStatus = "success"
	JobRunning  RcloneJobStatus = "running"
	JobNotFound RcloneJobStatus = "not_found"
)

// RcloneStatusOfJob extracts simple status from rclone job.
func (c *Client) RcloneStatusOfJob(job *models.Job) (status RcloneJobStatus) {
	status = JobRunning

	switch {
	case job == nil:
		status = JobNotFound
	case job.Finished && job.Success:
		status = JobSuccess
	case job.Finished && !job.Success:
		status = JobError
	}

	return
}

// RcloneTransfer represents a single file transfer in RcloneJobInfo Transferred.
type RcloneTransfer = models.Transfer

// RcloneDeleteJobStats deletes job stats group.
func (c *Client) RcloneDeleteJobStats(ctx context.Context, host string, jobID int64) error {
	p := operations.CoreStatsDeleteParams{
		Context: httpmw.ForceHost(ctx, host),
		StatsParams: &models.StatsParams{
			Group: rcloneDefaultGroup(jobID),
		},
	}
	_, err := c.agentOps.CoreStatsDelete(&p) //nolint:errcheck
	return err
}

// RcloneResetStats resets stats.
func (c *Client) RcloneResetStats(ctx context.Context, host string) error {
	p := operations.CoreStatsResetParams{
		Context: httpmw.ForceHost(ctx, host),
	}
	_, err := c.agentOps.CoreStatsReset(&p) //nolint:errcheck
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
		Context: httpmw.ForceHost(ctx, host),
		Copyfile: &models.MoveOrCopyFileOptions{
			DstFs:     dstFs,
			DstRemote: dstRemote,
			SrcFs:     srcFs,
			SrcRemote: srcRemote,
		},
	}
	_, err = c.agentOps.OperationsMovefile(&p)
	return err
}

// RcloneCopyFile copies file from srcRemotePath to dstRemotePath.
// Remotes need to be registered with the server first.
// Returns ID of the asynchronous job.
// Remote path format is "name:bucket/path".
// Both dstRemotePath and srRemotePath must point to a file.
func (c *Client) RcloneCopyFile(ctx context.Context, host, dstRemotePath, srcRemotePath string) (int64, error) {
	dstFs, dstRemote, err := rcloneSplitRemotePath(dstRemotePath)
	if err != nil {
		return 0, err
	}
	srcFs, srcRemote, err := rcloneSplitRemotePath(srcRemotePath)
	if err != nil {
		return 0, err
	}
	p := operations.OperationsCopyfileParams{
		Context: httpmw.ForceHost(ctx, host),
		Copyfile: &models.MoveOrCopyFileOptions{
			DstFs:     dstFs,
			DstRemote: dstRemote,
			SrcFs:     srcFs,
			SrcRemote: srcRemote,
		},
		Async: true,
	}
	resp, err := c.agentOps.OperationsCopyfile(&p)
	if err != nil {
		return 0, err
	}
	return resp.Payload.Jobid, nil
}

// RcloneCopyDir copies contents of the directory pointed by srcRemotePath to
// the directory pointed by dstRemotePath.
// Remotes need to be registered with the server first.
// Returns ID of the asynchronous job.
// Remote path format is "name:bucket/path".
func (c *Client) RcloneCopyDir(ctx context.Context, host, dstRemotePath, srcRemotePath string) (int64, error) {
	p := operations.SyncCopyParams{
		Context: httpmw.ForceHost(ctx, host),
		Copydir: operations.SyncCopyBody{
			SrcFs: srcRemotePath,
			DstFs: dstRemotePath,
		},
		Async: true,
	}
	resp, err := c.agentOps.SyncCopy(&p)
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
		Context: httpmw.ForceHost(ctx, host),
		Purge: &models.RemotePath{
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
		Context: httpmw.ForceHost(ctx, host),
		Deletefile: &models.RemotePath{
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
		Context: httpmw.ForceHost(ctx, host),
		About: &models.RemotePath{
			Fs: remotePath,
		},
	}
	resp, err := c.agentOps.OperationsAbout(&p)
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

// RcloneCat returns a content of a remote path.
// Only use that for small files, it loads the whole file to memory on a remote
// node and only then returns it. This is caused by rclone design.
func (c *Client) RcloneCat(ctx context.Context, host, remotePath string) ([]byte, error) {
	fs, remote, err := rcloneSplitRemotePath(remotePath)
	if err != nil {
		return nil, err
	}
	p := operations.OperationsCatParams{
		Context: httpmw.ForceHost(ctx, host),
		Cat: &models.RemotePath{
			Fs:     fs,
			Remote: remote,
		},
	}
	resp, err := c.agentOps.OperationsCat(&p)
	if err != nil {
		return nil, err
	}
	return resp.Payload.Content, nil
}

// RcloneListDirOpts specifies options for RcloneListDir.
type RcloneListDirOpts = models.ListOptionsOpt

// RcloneListDirItem represents a file in a listing with RcloneListDir.
type RcloneListDirItem = models.ListItem

// RcloneListDir lists contents of a directory specified by the path.
// Remote path format is "name:bucket/path".
// Listed item path is relative to the remote path root directory.
func (c *Client) RcloneListDir(ctx context.Context, host, remotePath string, opts *RcloneListDirOpts) ([]*models.ListItem, error) {
	empty := ""
	p := operations.OperationsListParams{
		Context: httpmw.ForceHost(ctx, host),
		ListOpts: &models.ListOptions{
			Fs:     &remotePath,
			Remote: &empty,
			Opt:    opts,
		},
	}
	resp, err := c.agentOps.OperationsList(&p)
	if err != nil {
		return nil, err
	}

	return resp.Payload.List, nil
}

// RcloneCheckPermissions checks if location is available for listing, getting,
// creating, and deleting objects.
func (c *Client) RcloneCheckPermissions(ctx context.Context, host, remotePath string) error {
	p := operations.OperationsCheckPermissionsParams{
		Context: httpmw.ForceHost(ctx, host),
		Fs: &models.RemotePath{
			Fs:     remotePath,
			Remote: "",
		},
	}
	_, err := c.agentOps.OperationsCheckPermissions(&p)
	return err
}

// rcloneSplitRemotePath splits string path into file system and file path.
func rcloneSplitRemotePath(remotePath string) (fs, path string, err error) {
	parts := strings.Split(remotePath, ":")
	if len(parts) != 2 {
		err = errors.New("remote path without file system name")
		return
	}
	if parts[1] == "" {
		err = errors.New("file path empty")
		return
	}

	fs = fmt.Sprintf("%s:%s", parts[0], filepath.Dir(parts[1]))
	path = filepath.Base(parts[1])
	return
}
