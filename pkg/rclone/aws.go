// Copyright (C) 2017 ScyllaDB

package rclone

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/rclone/rclone/fs"
)

// awsRegionFromMetadataAPI uses instance metadata API to fetch region of the
// running instance see
// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html
// Returns empty string if region can't be obtained for whatever reason.
func awsRegionFromMetadataAPI() string {
	const url = "http://169.254.169.254/latest/dynamic/instance-identity/document"

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		fs.Errorf(nil, "create metadata request: %+v", err)
		return ""
	}
	req.Header.Set("User-Agent", UserAgent())

	metadataClient := http.Client{
		Timeout: 2 * time.Second,
	}
	res, err := metadataClient.Do(req)
	if err != nil {
		fs.Debugf(nil, "AWS failed to fetch instance identity: %+v", err)
		return ""
	}
	defer res.Body.Close()

	metadata := struct {
		Region string `json:"region"`
	}{}
	if err := json.NewDecoder(res.Body).Decode(&metadata); err != nil {
		fs.Errorf(nil, "parse instance region: %+v", err)
		return ""
	}
	return metadata.Region
}
