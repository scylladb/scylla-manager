// Copyright (C) 2017 ScyllaDB

package scyllaclient

import (
	"context"
	"fmt"

	"github.com/scylladb/mermaid/scyllaclient/internal/rclone/client/operations"
	"github.com/scylladb/mermaid/scyllaclient/internal/rclone/models"
)

// S3Params defines parameters for S3 remote.
type S3Params struct {
	Provider          string `json:"provider,omitempty"`
	Region            string `json:"region,omitempty"`
	EnvAuth           bool   `json:"env_auth,omitempty"`
	AccessKey         string `json:"access_key,omitempty"`
	SecretAccessKey   string `json:"secret_access_key,omitempty"`
	Endpoint          string `json:"endpoint,omitempty"`
	DisableChecksum   string `json:"disable_checksum,omitempty"`
	UploadConcurrency int    `json:"upload_concurrency,omitempty"`
}

// RcloneRegisterS3Remote crates new AWS S3 based remote to the agent.
// After registration S3 remote can be referenced as "name:".
// Bucket can be referenced as "name:bucket".
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
func (c *Client) RcloneSetBandwidthLimit(ctx context.Context, host string, limit int) error {
	p := operations.CoreBwlimitParams{
		Context:       forceHost(ctx, host),
		BandwidthRate: &models.Bandwidth{Rate: fmt.Sprintf("%dM", limit)},
	}
	_, err := c.rcloneOpts.CoreBwlimit(&p) //nolint:errcheck
	return err
}

// RcloneJobStatus fetches information about the created job.
func (c *Client) RcloneJobStatus(ctx context.Context, host string, id int) (*models.Job, error) {
	p := operations.JobStatusParams{
		Context: forceHost(ctx, host),
		Jobid:   &models.Jobid{Jobid: int64(id)},
	}
	resp, err := c.rcloneOpts.JobStatus(&p)
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

// RcloneCopyFile moves file from the node to the remote destination specified
// by dstFilename.
// Remote needs to be registered first with the server.
// Returns ID of the asynchronous job.
func (c *Client) RcloneCopyFile(ctx context.Context, host string, dstFs, dstRemote, srcFs, srcRemote string) (int64, error) {
	p := operations.OperationsCopyfileParams{
		Context: forceHost(ctx, host),
		Copyfile: &models.CopyOptions{
			DstFs:     dstFs,
			DstRemote: dstRemote,
			SrcFs:     srcFs,
			SrcRemote: srcRemote,
		},
	}
	resp, err := c.rcloneOpts.OperationsCopyfile(&p)
	if err != nil {
		return 0, err
	}
	return resp.Payload.Jobid, nil
}

// RcloneCopyDir moves file from the node to the remote destination specified
// by dstDir.
// Remote needs to be registered first with the server.
// Returns ID of the asynchronous job.
func (c *Client) RcloneCopyDir(ctx context.Context, host string, dstFs, srcFs string) (int64, error) {
	p := operations.SyncCopyParams{
		Context: forceHost(ctx, host),
		Copydir: operations.SyncCopyBody{
			SrcFs: srcFs,
			DstFs: dstFs,
		},
	}
	resp, err := c.rcloneOpts.SyncCopy(&p)
	if err != nil {
		return 0, err
	}
	return resp.Payload.Jobid, nil
}

// RclonePurge removes a directory or container and all of its contents from
// the remote.
func (c *Client) RclonePurge(ctx context.Context, host string, fs, remote string) (int64, error) {
	p := operations.OperationsPurgeParams{
		Context: forceHost(ctx, host),
		Purge: &models.RemotePath{
			Fs:     fs,
			Remote: remote,
		},
	}
	resp, err := c.rcloneOpts.OperationsPurge(&p)
	if err != nil {
		return 0, err
	}
	return resp.Payload.Jobid, nil
}

// RcloneDiskUsage get disk space usage.
func (c *Client) RcloneDiskUsage(ctx context.Context, host string, fs, remote string) (*models.FileSystemDetails, error) {
	p := operations.OperationsAboutParams{
		Context: forceHost(ctx, host),
		About: &models.RemotePath{
			Fs:     fs,
			Remote: remote,
		},
	}
	resp, err := c.rcloneOpts.OperationsAbout(&p)
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}

// RcloneListDir lists contents of a directory specified by the path.
func (c *Client) RcloneListDir(ctx context.Context, host string, fs, remote string, recurse bool) ([]*models.ListItem, error) {
	p := operations.OperationsListParams{
		Context: forceHost(ctx, host),
		ListOpts: &models.ListOptions{
			Fs:     fs,
			Remote: remote,
			Opt: &models.ListOptionsOpt{
				Recurse: recurse,
			},
		},
	}
	resp, err := c.rcloneOpts.OperationsList(&p)
	if err != nil {
		return nil, err
	}
	return resp.Payload, nil
}
