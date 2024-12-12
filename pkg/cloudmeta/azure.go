// Copyright (C) 2024 ScyllaDB

package cloudmeta

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
)

// azureBaseURL is a base url of azure metadata service.
const azureBaseURL = "http://169.254.169.254/metadata"

// azureMetadata is a wrapper around azure metadata service.
type azureMetadata struct {
	client *http.Client

	baseURL string
}

// newAzureMetadata returns AzureMetadata service.
func newAzureMetadata(logger log.Logger) *azureMetadata {
	return &azureMetadata{
		client:  defaultClient(logger),
		baseURL: azureBaseURL,
	}
}

func defaultClient(logger log.Logger) *http.Client {
	client := retryablehttp.NewClient()

	client.RetryMax = 3
	client.RetryWaitMin = 500 * time.Millisecond
	client.RetryWaitMax = 5 * time.Second
	client.Logger = &logWrapper{
		logger: logger,
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	// we must not use proxy for the metadata requests - see https://learn.microsoft.com/en-us/azure/virtual-machines/instance-metadata-service?tabs=linux#proxies.
	transport.Proxy = nil

	client.HTTPClient = &http.Client{
		// Quite small timeout per request, because we have retries and also it's a local network call.
		Timeout:   1 * time.Second,
		Transport: transport,
	}
	return client.StandardClient()
}

// Metadata return InstanceMetadata from azure if available.
func (azure *azureMetadata) Metadata(ctx context.Context) (InstanceMetadata, error) {
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

func (azure *azureMetadata) getVMSize(ctx context.Context) (string, error) {
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

// logWrapper implements go-retryablehttp.LeveledLogger interface.
type logWrapper struct {
	logger log.Logger
}

// Info wraps logger.Info method.
func (log *logWrapper) Info(msg string, keyVals ...interface{}) {
	log.logger.Info(context.Background(), msg, keyVals...)
}

// Error wraps logger.Error method.
func (log *logWrapper) Error(msg string, keyVals ...interface{}) {
	log.logger.Error(context.Background(), msg, keyVals...)
}

// Warn wraps logger.Error method.
func (log *logWrapper) Warn(msg string, keyVals ...interface{}) {
	log.logger.Error(context.Background(), msg, keyVals...)
}

// Debug wraps logger.Debug method.
func (log *logWrapper) Debug(msg string, keyVals ...interface{}) {
	log.logger.Debug(context.Background(), msg, keyVals...)
}
