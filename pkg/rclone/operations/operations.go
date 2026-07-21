// Copyright (C) 2026 ScyllaDB

package operations

import (
	"context"
	stderr "errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/pkg/errors"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/operations"
	"github.com/rclone/rclone/fs/sync"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/scylladb/scylla-manager/v3/pkg/util/timeutc"
)

// OperationError wraps remote fs errors returned by CheckPermissions function
// and allows to set a custom message returned to user.
type OperationError struct {
	cause      error
	op         string
	statusCode int
}

func asOperationError(op string, l fs.Fs, err error) OperationError {
	statusCode := 400

	if l.Name() == "s3" {
		e, _ := ParseBackendXMLError(err) // nolint: errcheck
		if e != nil {
			err = e
		} else {
			statusCode = 500
		}
	}

	return OperationError{
		cause:      err,
		op:         op,
		statusCode: statusCode,
	}
}

func (e OperationError) Error() string {
	return "operation " + e.op + ": " + e.cause.Error()
}

func (e OperationError) String() string {
	return e.Error()
}

// StatusCode returns HTTP status code that should be returned for this error.
func (e OperationError) StatusCode() int {
	return e.statusCode
}

// CheckPermissions checks if file system is available for
// listing, getting, creating, and deleting objects.
// The remote parameter specifies a subdirectory within l
// where the test directory is created.
// Params retentionMode, overrideLock and eventBasedHold control whether
// retention lock and event based hold related permissions should be verified.
func CheckPermissions(ctx context.Context, l fs.Fs, remote, retentionMode string, overrideLock, eventBasedHold bool) error {
	// Disable retries for calls in permissions check.
	ctx = pacer.WithRetries(ctx, 1)

	// Create temp dir.
	tmpDir, err := os.MkdirTemp("", "scylla-manager-agent-")
	if err != nil {
		return errors.Wrap(err, "create local tmp directory")
	}
	defer os.RemoveAll(tmpDir) // nolint: errcheck

	// Create tmp file.
	var (
		testDirName  = filepath.Join(remote, filepath.Base(tmpDir))
		testFileName = "test"
	)
	if err := os.MkdirAll(filepath.Join(tmpDir, testDirName), os.ModePerm); err != nil {
		return errors.Wrap(err, "create local tmp subdirectory")
	}
	tmpFile := filepath.Join(tmpDir, testDirName, testFileName)
	if err := os.WriteFile(tmpFile, []byte{0}, os.ModePerm); err != nil {
		return errors.Wrap(err, "create local tmp file")
	}

	// Copy local tmp dir contents to the destination.
	f, err := fs.NewFs(ctx, tmpDir)
	if err != nil {
		return errors.Wrap(err, "init temp dir")
	}
	if err := copyTestFile(ctx, l, f); err != nil {
		return err
	}

	// List directory.
	{
		opts := operations.ListJSONOpt{
			Recurse:   false,
			NoModTime: true,
		}
		if err := operations.ListJSON(ctx, l, testDirName, &opts, func(_ *operations.ListJSONItem) error {
			return nil
		}); err != nil {
			return asOperationError("list", l, err)
		}
	}

	// Create and cat remote file.
	o, err := l.NewObject(ctx, filepath.Join(testDirName, testFileName))
	if err != nil {
		return errors.Wrap(err, "init remote temp file object")
	}
	{
		r, err := o.Open(ctx)
		if err != nil {
			return asOperationError("open", l, err)
		}
		_, readErr := io.Copy(io.Discard, r)
		if err := stderr.Join(readErr, r.Close()); err != nil {
			return asOperationError("read", l, err)
		}
	}

	checkDelete := true
	// Retention lock check.
	if retentionMode != "" {
		if err := checkRetentionLock(ctx, l, o, overrideLock); err != nil {
			return err
		}
		checkDelete = false
	}
	// Event based hold check.
	if eventBasedHold {
		if err := checkEventBasedHold(ctx, l, o); err != nil {
			return err
		}
		checkDelete = false
	}

	// Remove the file if eligible
	if checkDelete {
		if err := o.Remove(ctx); err != nil {
			return asOperationError("remove", l, err)
		}
	}

	// Cleanup.
	if err := operations.Purge(ctx, l, testDirName); err != nil {
		// As we already verified all permissions needed by SM to perform
		// a successful backup, we can just log an error here to allow
		// backup to proceed in case of unexpected and not critical error.
		if fs.GetConfig(context.TODO()).LogLevel >= fs.LogLevelWarning {
			fs.LogPrintf(fs.LogLevelWarning, l, "failed to remove test directory %q: %v", testDirName, err)
		}
	}

	return nil
}

// checkRetentionLock verifies retention lock and optional override unlocked permissions on object.
func checkRetentionLock(ctx context.Context, f fs.Fs, o fs.Object, overrideUnlocked bool) error {
	// Set retention lock (unlocked, now+1m).
	rs, ok := f.(fs.ObjectRetentionSetter)
	if !ok {
		return asOperationError("set-retention-lock", f, errors.Errorf("backend %q does not support retention lock", f.Name()))
	}
	info := fs.ObjectRetentionInfo{
		RetainUntil: timeutc.Now().Add(time.Minute),
		Mode:        fs.RetentionModeUnlocked,
	}
	if err := rs.SetObjectRetention(ctx, o.Remote(), info, false); err != nil {
		return asOperationError("set-retention-lock", f, err)
	}

	// Check applied retention lock.
	o, err := f.NewObject(ctx, o.Remote())
	if err != nil {
		return asOperationError("get-retention-lock", f, errors.Wrap(err, "reload object"))
	}
	rl, ok := o.(fs.ObjectRetentionInfoer)
	if !ok {
		return asOperationError("get-retention-lock", f, errors.New("object does not support retention lock info"))
	}
	info, err = rl.ObjectRetentionInfo(ctx)
	if err != nil {
		return asOperationError("get-retention-lock", f, err)
	}
	if info.Mode != fs.RetentionModeUnlocked {
		return asOperationError("get-retention-lock", f, errors.Errorf("expected retention lock %q, got %q", fs.RetentionModeUnlocked, info.Mode))
	}

	// Override retention lock (locked).
	if overrideUnlocked {
		info.Mode = fs.RetentionModeLocked
		if err := rs.SetObjectRetention(ctx, o.Remote(), info, true); err != nil {
			return asOperationError("override-unlocked", f, err)
		}
	}

	return nil
}

// checkEventBasedHold verifies event based hold permissions on object.
func checkEventBasedHold(ctx context.Context, f fs.Fs, o fs.Object) error {
	// Check default event based hold.
	es, ok := f.(fs.EventBasedHoldSetter)
	if !ok {
		return asOperationError("event-based-hold", f, errors.Errorf("backend %q does not support event-based hold", f.Name()))
	}
	eh, ok := o.(fs.EventBasedHolder)
	if !ok {
		return asOperationError("get-event-based-hold", f, errors.New("object does not support event-based hold"))
	}
	hold, err := eh.EventBasedHold(ctx)
	if err != nil {
		return asOperationError("get-event-based-hold", f, err)
	}
	if !hold {
		// SM can perform --retention-lock-mode=event-based-hold backup even without
		// default event based hold, as SM would just apply the holds itself in such case.
		// This is undesirable in most cases, as it results in additional per-object req.
		// In such case, warn with ad-hoc fs.Warnf implementation based on fs.Errorf.
		if fs.GetConfig(context.TODO()).LogLevel >= fs.LogLevelWarning {
			fs.LogPrintf(fs.LogLevelWarning, o, "Object doesn't have event based hold automatically applied")
		}
		// Set missing event based hold so that we can test its removal.
		if err := es.SetEventBasedHold(ctx, o.Remote(), true); err != nil {
			return asOperationError("set-event-based-hold", f, err)
		}
	}
	// Remove the event based hold.
	// With default bucket retention policy, this starts retention
	// on the object, so permission check cleanup has to remove it later.
	if err := es.SetEventBasedHold(ctx, o.Remote(), false); err != nil {
		return asOperationError("set-event-based-hold", f, err)
	}
	return nil
}

func copyTestFile(ctx context.Context, l, tmpFs fs.Fs) error {
	err := sync.CopyDir(ctx, l, tmpFs, true)
	if err != nil {
		// Special handling of permissions errors.
		if errors.Is(err, credentials.ErrNoValidProvidersFoundInChain) {
			return errors.New("no providers - attach IAM Role to EC2 instance or put your access keys to s3 section of /etc/scylla-manager-agent/scylla-manager-agent.yaml and restart agent") // nolint: lll
		}
		return asOperationError("put", l, err)
	}
	return nil
}
