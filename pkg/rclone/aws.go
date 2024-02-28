// Copyright (C) 2017 ScyllaDB

package rclone

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"time"

	"github.com/rclone/rclone/fs"
)

// awsRegionFromMetadataAPI uses instance metadata API v2 to fetch region of the
// running instance see https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instancedata-data-retrieval.html
// Returns empty string if region can't be obtained for whatever reason.
func awsRegionFromMetadataAPI() string {
	const (
		tokenUrl = "http://169.254.169.254/latest/api/token"
		docURL   = "http://169.254.169.254/latest/dynamic/instance-identity/document"
	)

	// Step 1: Request an IMDSv2 session token
	reqToken, err := http.NewRequestWithContext(context.Background(), http.MethodPut, tokenUrl, nil)
	if err != nil {
		fs.Errorf(nil, "create token request: %+v", err)
		return ""
	}
	reqToken.Header.Set("X-aws-ec2-metadata-token-ttl-seconds", "21600")
	tokenClient := http.Client{
		Timeout: 2 * time.Second,
	}
	resToken, err := tokenClient.Do(reqToken)
	if err != nil {
		fs.Errorf(nil, "IMDSv2 failed to fetch session token: %+v", err)
		return ""
	}
	defer resToken.Body.Close()

	if resToken.StatusCode != http.StatusOK {
		fs.Errorf(nil, "failed to retrieve session token: %s", resToken.Status)
		return ""
	}

	token, err := io.ReadAll(resToken.Body)
	if err != nil {
		fs.Errorf(nil, "Failed to read session token: %+v", err)
		return ""
	}

	// Step 2: Use the session token to retrieve instance metadata
	reqMetadata, err := http.NewRequestWithContext(context.Background(), http.MethodGet, docURL, nil)
	if err != nil {
		fs.Errorf(nil, "create metadata request: %+v", err)
		return ""
	}
	reqMetadata.Header.Set("X-aws-ec2-metadata-token", string(token))

	metadataClient := http.Client{
		Timeout: 2 * time.Second,
	}
	resMetadata, err := metadataClient.Do(reqMetadata)
	if err != nil {
		fs.Errorf(nil, "IMDSv2 failed to fetch instance identity: %+v", err)
		return ""
	}
	defer resMetadata.Body.Close()

	metadata := struct {
		Region string `json:"region"`
	}{}
	if err := json.NewDecoder(resMetadata.Body).Decode(&metadata); err != nil {
		fs.Errorf(nil, "parse instance region: %+v", err)
		return ""
	}

	return metadata.Region
}
