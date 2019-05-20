// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"context"
	"fmt"
	"path/filepath"

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
		Context: forceHost(ctx, host),
		Remote: &models.Remote{
			Name:       name,
			Type:       "s3",
			Parameters: params,
		},
	}
	_, err := c.rcloneOpts.ConfigCreate(&p) //nolint:errcheck
	return err
}

// RcloneSetBandwidthLimit sets bandwidth limit of all the current and future
// transfers performed under current client session.
// Limit is expressed in MiB per second.
// To turn off limitation set it to 0.
func (c *Client) RcloneSetBandwidthLimit(ctx context.Context, host string, limit int) error {
	p := operations.CoreBwlimitParams{
		Context:       forceHost(ctx, host),
		BandwidthRate: &models.Bandwidth{Rate: fmt.Sprintf("%dM", limit)},
	}
	_, err := c.rcloneOpts.CoreBwlimit(&p) //nolint:errcheck
	return err
}

// RcloneJobStatus fetches information about the created job.
func (c *Client) RcloneJobStatus(ctx context.Context, host string, id int64) (*models.Job, error) {
	p := operations.JobStatusParams{
		Context: forceHost(ctx, host),
		Jobid:   &models.Jobid{Jobid: id},
	}
	resp, err := c.rcloneOpts.JobStatus(&p)
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

// RcloneCopyFile copies file from the srcRemotePath to dstRemotePath.
// Remotes need to be registered with the server first.
// Returns ID of the asynchronous job.
// Remote path format is "name:bucket/path" with exception of local file system
// which is just path to the file.
// Both dstRemotePath and srRemotePath must point to a file.
func (c *Client) RcloneCopyFile(ctx context.Context, host string, dstRemotePath, srcRemotePath string) (int64, error) {
	p := operations.OperationsCopyfileParams{
		Context: forceHost(ctx, host),
		Copyfile: &models.CopyOptions{
			DstFs:     filepath.Dir(dstRemotePath),
			DstRemote: filepath.Base(dstRemotePath),
			SrcFs:     filepath.Dir(srcRemotePath),
			SrcRemote: filepath.Base(srcRemotePath),
		},
		Async: true,
	}
	resp, err := c.rcloneOpts.OperationsCopyfile(&p)
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
func (c *Client) RcloneCopyDir(ctx context.Context, host string, dstRemotePath, srcRemotePath string) (int64, error) {
	p := operations.SyncCopyParams{
		Context: forceHost(ctx, host),
		Copydir: operations.SyncCopyBody{
			SrcFs: srcRemotePath,
			DstFs: dstRemotePath,
		},
		Async: true,
	}
	resp, err := c.rcloneOpts.SyncCopy(&p)
	if err != nil {
		return 0, err
	}
	return resp.Payload.Jobid, nil
}

// RcloneDeleteDir removes a directory or container and all of its contents from
// the remote.
// Remote path format is "name:bucket/path" with exception of local file system
// which is just path to the directory.
func (c *Client) RcloneDeleteDir(ctx context.Context, host string, remotePath string) (int64, error) {
	p := operations.OperationsPurgeParams{
		Context: forceHost(ctx, host),
		Purge: &models.RemotePath{
			Fs:     filepath.Dir(remotePath),
			Remote: filepath.Base(remotePath),
		},
		Async: true,
	}
	resp, err := c.rcloneOpts.OperationsPurge(&p)
	if err != nil {
		return 0, err
	}
	return resp.Payload.Jobid, nil
}

// RcloneDeleteFile removes the single file pointed to by remotePath
// Remote path format is "name:bucket/path" with exception of local file system
// which is just path to the directory.
func (c *Client) RcloneDeleteFile(ctx context.Context, host string, remotePath string) (int64, error) {
	p := operations.OperationsDeletefileParams{
		Context: forceHost(ctx, host),
		Deletefile: &models.RemotePath{
			Fs:     filepath.Dir(remotePath),
			Remote: filepath.Base(remotePath),
		},
		Async: true,
	}
	resp, err := c.rcloneOpts.OperationsDeletefile(&p)
	if err != nil {
		return 0, err
	}
	return resp.Payload.Jobid, nil
}

// RcloneDiskUsage get disk space usage.
// Remote path format is "name:bucket/path" with exception of local file system
// which is just path to the directory.
func (c *Client) RcloneDiskUsage(ctx context.Context, host string, remotePath string) (*models.FileSystemDetails, error) {
	p := operations.OperationsAboutParams{
		Context: forceHost(ctx, host),
		About: &models.RemotePath{
			Fs: remotePath,
		},
	}
	resp, err := c.rcloneOpts.OperationsAbout(&p)
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

// RcloneListDir lists contents of a directory specified by the path.
// Remote path format is "name:bucket/path" with exception of local file system
// which is just path to the directory.
// Listed item path is relative to the remote path root directory.
func (c *Client) RcloneListDir(ctx context.Context, host, remotePath string, recurse bool) ([]*models.ListItem, error) {
	empty := ""
	p := operations.OperationsListParams{
		Context: forceHost(ctx, host),
		ListOpts: &models.ListOptions{
			Fs:     &remotePath,
			Remote: &empty,
			Opt: &models.ListOptionsOpt{
				Recurse: recurse,
			},
		},
	}
	resp, err := c.rcloneOpts.OperationsList(&p)
	if err != nil {
		return nil, err
	}
	return resp.Payload.List, nil
}
