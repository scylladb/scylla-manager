// Copyright (C) 2026 ScyllaDB

package testutils

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/scylladb/scylla-manager/backupspec"
	"github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util2/slices"
)

var (
	flagS3Provider          = flag.String("s3-provider", "", "test S3 instance provider")
	flagS3Endpoint          = flag.String("s3-endpoint", "", "test S3 instance endpoint")
	flagS3AccessKeyID       = flag.String("s3-access-key-id", "", "test S3 instance access key")
	flagS3SecretAccessKey   = flag.String("s3-secret-access-key", "", "test S3 instance secret")
	flagGCSDataDir          = flag.String("gcs-data-dir", "./testing/fake-gcs/data", "path to test GCS instance root data dir")
	flagGCSEndpoint         = flag.String("gcs-endpoint", "", "test GCS instance endpoint")
	flagGCSAnonymous        = flag.String("gcs-anonymous", "true", "test GCS instance anonymous access")
	flagLocalStorageDataDir = flag.String("localstorage-data-dir", "./testing/localstorage/data", "path to test localstorage root data dir")
)

// InitBucket recreates a local bucket for testconfig.BackupProvider.
func InitBucket(t *testing.T, bucket string) {
	t.Helper()

	if !flag.Parsed() {
		flag.Parse()
	}

	switch testconfig.BackupProvider() {
	case backupspec.S3:
		S3InitBucket(t, bucket)
	case backupspec.GCS:
		GCSInitBucket(t, bucket)
	case backupspec.LocalStorage:
		LocalStorageInitBucket(t, bucket)
	default:
		t.Fatalf("unsupported provider %s", testconfig.BackupProvider())
	}
}

// LocalStorageInitBucket recreates a local bucket if localstorage-data-dir flag is specified.
func LocalStorageInitBucket(t *testing.T, bucket string) {
	t.Helper()

	if !flag.Parsed() {
		flag.Parse()
	}
	if *flagLocalStorageDataDir == "" {
		t.Logf("No localstorage data dir specified, skipped clearing bucket %s", bucket)
		return
	}

	initDir(t, filepath.Join(*flagLocalStorageDataDir, bucket))
}

// S3InitBucket recreates a bucket in test S3 instance.
func S3InitBucket(t *testing.T, bucket string) {
	t.Helper()

	if !flag.Parsed() {
		flag.Parse()
	}

	client := newTestS3Client(t)
	ctx := t.Context()
	bucketPtr := new(bucket)

	_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: bucketPtr})
	if err != nil && !isNoSuchBucketErr(err) {
		t.Fatal(err)
	}
	if err == nil {
		abortS3MultipartUploads(ctx, t, client, bucket)
		clearS3Bucket(ctx, t, client, bucket)
		if _, err := client.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: bucketPtr}); err != nil {
			t.Fatal(err)
		}
		if err := s3.NewBucketNotExistsWaiter(client).Wait(ctx, &s3.HeadBucketInput{Bucket: bucketPtr}, time.Minute); err != nil {
			t.Fatal(err)
		}
	}

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: bucketPtr})
	if err != nil {
		t.Fatal(err)
	}
	if err := s3.NewBucketExistsWaiter(client).Wait(ctx, &s3.HeadBucketInput{Bucket: bucketPtr}, time.Minute); err != nil {
		t.Fatal(err)
	}
}

func newTestS3Client(t *testing.T) *s3.Client {
	t.Helper()

	if *flagS3Endpoint == "" || *flagS3AccessKeyID == "" || *flagS3SecretAccessKey == "" {
		t.Fatalf("s3 endpoint and credentials must be specified")
	}

	config := aws.Config{
		BaseEndpoint: flagS3Endpoint,
		Region:       "us-east-1",
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{
				AccessKeyID:     *flagS3AccessKeyID,
				SecretAccessKey: *flagS3SecretAccessKey,
			}, nil
		}),
		HTTPClient: &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		}}},
	}
	return s3.NewFromConfig(config, func(o *s3.Options) {
		o.UsePathStyle = true
	})
}

func abortS3MultipartUploads(ctx context.Context, t *testing.T, client *s3.Client, bucket string) {
	t.Helper()

	input := &s3.ListMultipartUploadsInput{Bucket: new(bucket)}
	var uploads []types.MultipartUpload
	for {
		out, err := client.ListMultipartUploads(ctx, input)
		if err != nil {
			t.Fatal(err)
		}
		uploads = append(uploads, out.Uploads...)

		if out.IsTruncated == nil || !*out.IsTruncated {
			break
		}
		input.KeyMarker = out.NextKeyMarker
		input.UploadIdMarker = out.NextUploadIdMarker
	}

	for _, upload := range uploads {
		_, err := client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   new(bucket),
			Key:      upload.Key,
			UploadId: upload.UploadId,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func clearS3Bucket(ctx context.Context, t *testing.T, client *s3.Client, bucket string) {
	t.Helper()

	input := &s3.ListObjectsV2Input{Bucket: new(bucket)}
	var objects []types.ObjectIdentifier
	for {
		out, err := client.ListObjectsV2(ctx, input)
		if err != nil {
			t.Fatal(err)
		}
		objects = append(objects, slices.Map(out.Contents, func(o types.Object) types.ObjectIdentifier {
			return types.ObjectIdentifier{Key: o.Key}
		})...)

		if out.IsTruncated == nil || !*out.IsTruncated {
			break
		}
		input.ContinuationToken = out.NextContinuationToken
	}

	for len(objects) > 0 {
		batchSize := min(1000, len(objects))
		batch := objects[:batchSize]
		objects = objects[batchSize:]
		_, err := client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
			Bucket: new(bucket),
			Delete: &types.Delete{Objects: batch, Quiet: new(true)},
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

func isNoSuchBucketErr(err error) bool {
	if err == nil {
		return false
	}
	if _, ok := errors.AsType[*types.NoSuchBucket](err); ok { // nolint: errcheck
		return true
	}
	if apiErr, ok := errors.AsType[smithy.APIError](err); ok && (apiErr.ErrorCode() == "NoSuchBucket" || apiErr.ErrorCode() == "NotFound") {
		return true
	}
	return false
}

// GCSInitBucket recreates a local bucket if gcs-data-dir flag is specified.
func GCSInitBucket(t *testing.T, bucket string) {
	t.Helper()

	if !flag.Parsed() {
		flag.Parse()
	}
	if *flagGCSDataDir == "" {
		t.Logf("No gcs data dir specified, skipped clearing bucket %s", bucket)
		return
	}

	initDir(t, filepath.Join(*flagGCSDataDir, bucket))
}

func initDir(t *testing.T, dir string) {
	t.Helper()

	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
}

// S3Credentials returns provider, endpoint, and credentials to test S3 instance.
func S3Credentials() (provider, endpoint, accessKeyID, secretAccessKey string) {
	if !flag.Parsed() {
		flag.Parse()
	}
	return *flagS3Provider, *flagS3Endpoint, *flagS3AccessKeyID, *flagS3SecretAccessKey
}

// GCSCredentials returns endpoint flag for the test GCS instance.
func GCSCredentials() (endpoint, anonymous string) {
	if !flag.Parsed() {
		flag.Parse()
	}
	return *flagGCSEndpoint, *flagGCSAnonymous
}
