// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/scylladb/mermaid/internal/httputil/middleware"
	"github.com/scylladb/mermaid/scyllaclient/internal/rclone/client/operations"
	"github.com/scylladb/mermaid/scyllaclient/internal/rclone/models"
)

// S3Params defines parameters for S3 remote.
type S3Params struct {
	Provider          string `json:"provider,omitempty"`
	Region            string `json:"region,omitempty"`
	EnvAuth           bool   `json:"env_auth,omitempty"`
	AccessKeyID       string `json:"access_key_id,omitempty"`
	SecretAccessKey   string `json:"secret_access_key,omitempty"`
	Endpoint          string `json:"endpoint,omitempty"`
	DisableChecksum   bool   `json:"disable_checksum,omitempty"`
	UploadConcurrency int    `json:"upload_concurrency,omitempty"`
}

// RcloneRegisterS3Remote registers new S3 based remote with the rclone
// configuration registry running on the agent.
// After registering the remote with "name", bucket can be referenced in the
// format "name:bucket-name".
func (c *Client) RcloneRegisterS3Remote(ctx context.Context, host, name string, params S3Params) error {
	p := operations.ConfigCreateParams{
		Context: middleware.ForceHost(ctx, host),
		Remote: &models.Remote{
			Name:       name,
			Type:       "s3",
			Parameters: params,
		},
	}
	_, err := c.rcloneOps.ConfigCreate(&p) //nolint:errcheck
	return err
}

// RcloneSetBandwidthLimit sets bandwidth limit of all the current and future
// transfers performed under current client session.
// Limit is expressed in MiB per second.
// To turn off limitation set it to 0.
func (c *Client) RcloneSetBandwidthLimit(ctx context.Context, host string, limit int) error {
	p := operations.CoreBwlimitParams{
		Context:       middleware.ForceHost(ctx, host),
		BandwidthRate: &models.Bandwidth{Rate: fmt.Sprintf("%dM", limit)},
	}
	_, err := c.rcloneOps.CoreBwlimit(&p) //nolint:errcheck
	return err
}

// RcloneJobStatus fetches information about the created job.
func (c *Client) RcloneJobStatus(ctx context.Context, host string, id int64) (*models.Job, error) {
	p := operations.JobStatusParams{
		Context: middleware.ForceHost(ctx, host),
		Jobid:   &models.Jobid{Jobid: id},
	}
	resp, err := c.rcloneOps.JobStatus(&p)
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

// RcloneJobStop stops running job.
func (c *Client) RcloneJobStop(ctx context.Context, host string, id int64) error {
	p := operations.JobStopParams{
		Context: middleware.ForceHost(ctx, host),
		Jobid:   &models.Jobid{Jobid: id},
	}
	_, err := c.rcloneOps.JobStop(&p) //nolint:errcheck
	return err
}

// RcloneTransferred fetches information about all completed transfers.
func (c *Client) RcloneTransferred(ctx context.Context, host string, group string) ([]*models.Transfer, error) {
	p := operations.CoreTransferredParams{
		Context: middleware.ForceHost(ctx, host),
		StatsParams: &models.StatsParams{
			Group: group,
		},
	}
	resp, err := c.rcloneOps.CoreTransferred(&p)
	if err != nil {
		return nil, err
	}
	return resp.Payload.Transferred, nil
}

// RcloneStats fetches stats about current transfers.
func (c *Client) RcloneStats(ctx context.Context, host string, group string) (*models.Stats, error) {
	p := operations.CoreStatsParams{
		Context: middleware.ForceHost(ctx, host),
		StatsParams: &models.StatsParams{
			Group: group,
		},
	}
	resp, err := c.rcloneOps.CoreStats(&p)
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

// RcloneDefaultGroup returns default group name based on job id.
func RcloneDefaultGroup(jobID int64) string {
	return fmt.Sprintf("job/%d", jobID)
}

// RcloneStatsReset resets stats.
func (c *Client) RcloneStatsReset(ctx context.Context, host string, group string) error {
	p := operations.CoreStatsResetParams{
		Context: middleware.ForceHost(ctx, host),
		StatsParams: &models.StatsParams{
			Group: group,
		},
	}
	_, err := c.rcloneOps.CoreStatsReset(&p) //nolint:errcheck
	return err
}

// RcloneCopyFile copies file from the srcRemotePath to dstRemotePath.
// Remotes need to be registered with the server first.
// Returns ID of the asynchronous job.
// Remote path format is "name:bucket/path" with exception of local file system
// which is just path to the file.
// Both dstRemotePath and srRemotePath must point to a file.
func (c *Client) RcloneCopyFile(ctx context.Context, host string, dstRemotePath, srcRemotePath string) (int64, error) {
	p := operations.OperationsCopyfileParams{
		Context: middleware.ForceHost(ctx, host),
		Copyfile: &models.CopyOptions{
			DstFs:     filepath.Dir(dstRemotePath),
			DstRemote: filepath.Base(dstRemotePath),
			SrcFs:     filepath.Dir(srcRemotePath),
			SrcRemote: filepath.Base(srcRemotePath),
		},
		Async: true,
	}
	resp, err := c.rcloneOps.OperationsCopyfile(&p)
	if err != nil {
		return 0, err
	}
	return resp.Payload.Jobid, nil
}

// RcloneCopyDir copies contents of the directory pointed by srcRemotePath to
// the directory pointed by dstRemotePath.
// Remotes need to be registered with the server first.
// Returns ID of the asynchronous job.
// Remote path format is "name:bucket/path" with exception of local file system
// which is just path to the directory.
// To exclude files by filename pattern (just filename without directory path)
// pass them as variadic arguments.
func (c *Client) RcloneCopyDir(ctx context.Context, host string, dstRemotePath, srcRemotePath string, exclude ...string) (int64, error) {
	p := operations.SyncCopyParams{
		Context: middleware.ForceHost(ctx, host),
		Copydir: operations.SyncCopyBody{
			SrcFs:   srcRemotePath,
			DstFs:   dstRemotePath,
			Exclude: exclude,
		},
		Async: true,
	}
	resp, err := c.rcloneOps.SyncCopy(&p)
	if err != nil {
		return 0, err
	}
	return resp.Payload.Jobid, nil
}

// RcloneDeleteDir removes a directory or container and all of its contents
// from the remote.
// Remote path format is "name:bucket/path" with exception of local file system
// which is just path to the directory.
func (c *Client) RcloneDeleteDir(ctx context.Context, host string, remotePath string) error {
	p := operations.OperationsPurgeParams{
		Context: middleware.ForceHost(ctx, host),
		Purge: &models.RemotePath{
			Fs:     filepath.Dir(remotePath),
			Remote: filepath.Base(remotePath),
		},
		Async: false,
	}
	_, err := c.rcloneOps.OperationsPurge(&p) // nolint: errcheck
	return err
}

// RcloneDeleteFile removes the single file pointed to by remotePath
// Remote path format is "name:bucket/path" with exception of local file system
// which is just path to the directory.
func (c *Client) RcloneDeleteFile(ctx context.Context, host string, remotePath string) error {
	p := operations.OperationsDeletefileParams{
		Context: middleware.ForceHost(ctx, host),
		Deletefile: &models.RemotePath{
			Fs:     filepath.Dir(remotePath),
			Remote: filepath.Base(remotePath),
		},
		Async: false,
	}
	_, err := c.rcloneOps.OperationsDeletefile(&p) // nolint: errcheck
	return err
}

// RcloneDiskUsage get disk space usage.
// Remote path format is "name:bucket/path" with exception of local file system
// which is just path to the directory.
func (c *Client) RcloneDiskUsage(ctx context.Context, host string, remotePath string) (*models.FileSystemDetails, error) {
	p := operations.OperationsAboutParams{
		Context: middleware.ForceHost(ctx, host),
		About: &models.RemotePath{
			Fs: remotePath,
		},
	}
	resp, err := c.rcloneOps.OperationsAbout(&p)
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

// RcloneCat returns a content of a remote path.
// Only use that for small files, it loads the whole file to memory on a remote
// node and only then returns it. This is caused by rclone design.
func (c *Client) RcloneCat(ctx context.Context, host string, remotePath string) ([]byte, error) {
	p := operations.OperationsCatParams{
		Context: middleware.ForceHost(ctx, host),
		Cat: &models.RemotePath{
			Fs: remotePath,
		},
	}
	resp, err := c.rcloneOps.OperationsCat(&p)
	if err != nil {
		return nil, err
	}
	return resp.Payload.Content, nil
}

// RcloneListDir lists contents of a directory specified by the path.
// Remote path format is "name:bucket/path" with exception of local file system
// which is just path to the directory.
// Listed item path is relative to the remote path root directory.
func (c *Client) RcloneListDir(ctx context.Context, host, remotePath string, recurse bool) ([]*models.ListItem, error) {
	empty := ""
	p := operations.OperationsListParams{
		Context: middleware.ForceHost(ctx, host),
		ListOpts: &models.ListOptions{
			Fs:     &remotePath,
			Remote: &empty,
			Opt: &models.ListOptionsOpt{
				Recurse: recurse,
			},
		},
	}
	resp, err := c.rcloneOps.OperationsList(&p)
	if err != nil {
		return nil, err
	}
	return resp.Payload.List, nil
}

// TransferredByFilename returns all transferred entries for the file.
func TransferredByFilename(filename string, transferred []*models.Transfer) []*models.Transfer {
	var out []*models.Transfer
	for _, tr := range transferred {
		if tr.Name == filename {
			out = append(out, tr)
		}
	}
	return out
}
