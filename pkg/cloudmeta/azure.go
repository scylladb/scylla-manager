// Copyright (C) 2024 ScyllaDB

package cloudmeta

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

// AzureBaseURL is a base url of azure metadata service.
const AzureBaseURL = "http://169.254.169.254/metadata"

// AzureMetadata is a wrapper around azure metadata service.
type AzureMetadata struct {
	client *http.Client

	baseURL string
}

// NewAzureMetadata returns AzureMetadata service.
func NewAzureMetadata() *AzureMetadata {
	return &AzureMetadata{
		client:  defaultClient(),
		baseURL: AzureBaseURL,
	}
}

func defaultClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// we must not use proxy for the metadata requests - see https://learn.microsoft.com/en-us/azure/virtual-machines/instance-metadata-service?tabs=linux#proxies.
	transport.Proxy = nil
	return &http.Client{
		// setting some generous timeout, it can be overwritten by using context.WithTimeout.
		Timeout:   10 * time.Second,
		Transport: transport,
	}
}

// Metadata return InstanceMetadata from azure if available.
func (azure *AzureMetadata) Metadata(ctx context.Context) (InstanceMetadata, error) {
	vmSize, err := azure.getVMSize(ctx)
	if err != nil {
		return InstanceMetadata{}, errors.Wrap(err, "azure.getVMSize")
	}
	if vmSize == "" {
		return InstanceMetadata{}, errors.New("azure vmSize is empty")
	}
	return InstanceMetadata{
		CloudProvider: CloudProviderAzure,
		InstanceType:  vmSize,
	}, nil
}

// azureAPIVersion should be present in every request to metadata service in query parameter.
const azureAPIVersion = "2023-07-01"

func (azure *AzureMetadata) getVMSize(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, azure.baseURL+"/instance", http.NoBody)
	if err != nil {
		return "", errors.Wrap(err, "http new request")
	}

	// Setting required headers and query parameters - https://learn.microsoft.com/en-us/azure/virtual-machines/instance-metadata-service?tabs=linux#security-and-authentication.
	req.Header.Add("Metadata", "true")
	query := req.URL.Query()
	query.Add("api-version", azureAPIVersion)
	req.URL.RawQuery = query.Encode()

	resp, err := azure.client.Do(req)
	if err != nil {
		return "", errors.Wrap(err, "azure.client.Do")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", errors.Errorf("status code (%d) != 200", resp.StatusCode)
	}

	var data azureMetadataResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return "", errors.Wrap(err, "decode json")
	}

	return data.Compute.VMSize, nil
}

// azureMetadataResponse represents azure metadata service response.
// full response specification can be found here - https://learn.microsoft.com/en-us/azure/virtual-machines/instance-metadata-service?tabs=linux#response-1.
type azureMetadataResponse struct {
	Compute azureCompute `json:"compute"`
}

type azureCompute struct {
	VMSize string `json:"vmSize"`
}
