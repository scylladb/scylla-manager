// Copyright (C) 2017 ScyllaDB

package rcserver

import (
	"encoding/json"
	"net/http"
	"os"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/scylladb/mermaid/rclone"
)

func awsS3Region() string {
	region := os.Getenv("AWS_S3_REGION")
	if region != "" {
		return region
	}
	return fetchRegionFromMetadataAPI()
}

// Uses Instance Metadata API to fetch region of the running instance.
// https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html
// Returns empty string if region can't be obtained for whatever reason.
func fetchRegionFromMetadataAPI() string {
	url := "http://169.254.169.254/latest/dynamic/instance-identity/document"

	metadataClient := http.Client{
		Timeout: time.Second * 2,
	}

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		fs.Errorf(nil, "create metadata request: %+v", err)
		return ""
	}

	req.Header.Set("User-Agent", rclone.UserAgent())

	res, err := metadataClient.Do(req)
	if err != nil {
		fs.Errorf(nil, "fetch instance identity: %+v", err)
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
