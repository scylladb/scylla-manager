// Copyright (C) 2026 ScyllaDB

//go:build all || integration

package scyllaclient_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"

	. "github.com/scylladb/scylla-manager/v3/pkg/testutils"
	. "github.com/scylladb/scylla-manager/v3/pkg/testutils/testconfig"
	"github.com/scylladb/scylla-manager/v3/pkg/util/pointer"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/agent/client/operations"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/agent/models"
)

// TestRcloneLocaldirPrefixCollisionIntegration validates that the localdir
// backend rejects paths that share a byte prefix with the registered jail root
// but are not actual descendants.
//
// The Agent registers the Scylla data directory (/var/lib/scylla/data) as
// "Jailed Scylla data". The vulnerability allows an authenticated caller to
// access /var/lib/scylla/data-<anything> because strings.HasPrefix matches
// the prefix "/var/lib/scylla/data" in both "/var/lib/scylla/data/subdir" and
// "/var/lib/scylla/data-sibling".
//
// This test makes real HTTP requests to the running agent and is expected to
// FAIL before the fix is applied.
// Reference: CLOUD-3190
func TestRcloneLocaldirPrefixCollisionIntegration(t *testing.T) {
	const (
		scyllaDataDir = "/var/lib/scylla/data"
		// Sibling directory that shares byte prefix with the data dir.
		siblingDir = "/var/lib/scylla/data-security-test"
	)

	testHost := ManagedClusterHost()
	ctx := context.Background()

	// Raw HTTP client for direct agent API calls that bypass the Go client's
	// path splitting (which never sends an absolute path as fs).
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // nolint: gosec
		},
	}
	agentBaseURL := fmt.Sprintf("https://%s/agent/rclone", net.JoinHostPort(testHost, "10001"))
	authToken := AgentAuthToken()

	// Helper for making authenticated POST requests to the agent rclone API.
	agentPost := func(t *testing.T, endpoint string, body any) (int, string) {
		t.Helper()
		b, _ := json.Marshal(body)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, agentBaseURL+"/"+endpoint, bytes.NewReader(b))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+authToken)
		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		respBody, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(respBody)
	}

	// Setup: create a source file inside the jail on the agent node.
	setupCmd := strings.Join([]string{
		"mkdir -p " + scyllaDataDir + "/security_test_src",
		"echo -n 'jail content' > " + scyllaDataDir + "/security_test_src/source.txt",
		"rm -rf " + siblingDir,
	}, " && ")
	if _, _, err := ExecOnHost(testHost, setupCmd); err != nil {
		t.Fatal("setup failed:", err)
	}

	// Cleanup after test.
	defer func() {
		cleanupCmd := strings.Join([]string{
			"rm -rf " + scyllaDataDir + "/security_test_src",
			"rm -rf " + siblingDir,
			// Clean up any jailed rewrites that may have been created inside
			// the data directory to avoid confusing Scylla's table scanner.
			// With the fix applied, paths like /var/lib/scylla/data-security-test
			// get rewritten to /var/lib/scylla/data/var/lib/scylla/data-security-test.
			"rm -rf " + scyllaDataDir + "/var",
			"rm -rf " + scyllaDataDir + "/tmp",
		}, " && ")
		_, _, _ = ExecOnHost(testHost, cleanupCmd)
	}()

	t.Run("copyfile via raw HTTP rejects prefix-collision sibling", func(t *testing.T) {
		// Directly craft the request with the absolute path in the fs (root)
		// portion, which is how an attacker would exploit the vulnerability.
		// srcFs = "data:" (root stays inside jail)
		// dstFs = "data:/var/lib/scylla/data-security-test" (prefix collision!)
		status, respBody := agentPost(t, "operations/copyfile", map[string]interface{}{
			"srcFs":     "data:",
			"srcRemote": "security_test_src/source.txt",
			"dstFs":     "data:" + siblingDir,
			"dstRemote": "escaped.txt",
		})

		// Verify that the file was NOT created outside the jail.
		stdout, _, execErr := ExecOnHost(testHost, "cat "+siblingDir+"/escaped.txt 2>/dev/null || echo 'FILE_NOT_FOUND'")
		if execErr != nil {
			t.Fatal("exec failed:", execErr)
		}
		if !strings.Contains(stdout, "FILE_NOT_FOUND") {
			t.Fatalf("SECURITY VIOLATION: file was created outside jail at %s/escaped.txt "+
				"(HTTP status=%d, body=%s)", siblingDir, status, respBody)
		}
	})

	t.Run("purge via raw HTTP rejects prefix-collision sibling", func(t *testing.T) {
		// Create a sibling directory with protected content.
		setupCmd := "mkdir -p " + siblingDir + " && echo -n 'protected' > " + siblingDir + "/protected.txt"
		if _, _, err := ExecOnHost(testHost, setupCmd); err != nil {
			t.Fatal("setup failed:", err)
		}

		// Attempt to purge the sibling directory via prefix collision.
		agentPost(t, "operations/purge", map[string]interface{}{
			"fs":     "data:" + siblingDir,
			"remote": "",
		})

		// Verify the sibling directory still exists.
		stdout, _, execErr := ExecOnHost(testHost, "cat "+siblingDir+"/protected.txt 2>/dev/null || echo 'FILE_NOT_FOUND'")
		if execErr != nil {
			t.Fatal("exec failed:", execErr)
		}
		if strings.Contains(stdout, "FILE_NOT_FOUND") {
			t.Fatalf("SECURITY VIOLATION: prefix-collision sibling %s was purged from outside the jail", siblingDir)
		}
	})

	t.Run("list via raw HTTP rejects prefix-collision sibling", func(t *testing.T) {
		// Ensure sibling exists with content.
		setupCmd := "mkdir -p " + siblingDir + " && echo -n 'secret' > " + siblingDir + "/secret.txt"
		if _, _, err := ExecOnHost(testHost, setupCmd); err != nil {
			t.Fatal("setup failed:", err)
		}

		// Real RcloneListDir can't be called here, as it properly
		// divides fs from remote and sends them separately.
		status, respBody := agentPost(t, "operations/list", &models.ListOptions{
			Fs:     pointer.StringPtr("data:" + siblingDir),
			Remote: pointer.StringPtr(""),
		})

		// Verify that no sibling content was listed.
		if status == http.StatusOK {
			var out operations.OperationsListOKBody
			if err := json.Unmarshal([]byte(respBody), &out); err != nil {
				t.Fatalf("decode list response: %s (body=%s)", err, respBody)
			}
			if len(out.List) > 0 {
				t.Fatalf("SECURITY VIOLATION: listed %d items from prefix-collision sibling %s outside the jail "+
					"(first item=%s)", len(out.List), siblingDir, out.List[0].Path)
			}
		} else if status != http.StatusNotFound {
			t.Fatalf("unexpected status %d for list of prefix-collision sibling %s (body=%s)", status, siblingDir, respBody)
		}
	})

	t.Run("negative control unrelated path stays jailed", func(t *testing.T) {
		// A path that does NOT share the jail prefix should be rewritten
		// under the jail. /tmp does not start with /var/lib/scylla/data.
		unrelatedDir := "/tmp/scylla-security-test-unrelated"
		cleanCmd := "rm -rf " + unrelatedDir
		if _, _, err := ExecOnHost(testHost, cleanCmd); err != nil {
			t.Fatal("cleanup failed:", err)
		}
		defer func() { _, _, _ = ExecOnHost(testHost, cleanCmd) }()
		// Also clean up inside the jail where the path gets rewritten to.
		jailedPath := scyllaDataDir + unrelatedDir
		defer func() { _, _, _ = ExecOnHost(testHost, "rm -rf "+jailedPath) }()

		agentPost(t, "operations/copyfile", map[string]interface{}{
			"srcFs":     "data:",
			"srcRemote": "security_test_src/source.txt",
			"dstFs":     "data:" + unrelatedDir,
			"dstRemote": "marker.txt",
		})

		// Verify the file was NOT created at the unrelated outside path.
		stdout, _, execErr := ExecOnHost(testHost, "cat "+unrelatedDir+"/marker.txt 2>/dev/null || echo 'FILE_NOT_FOUND'")
		if execErr != nil {
			t.Fatal("exec failed:", execErr)
		}
		if !strings.Contains(stdout, "FILE_NOT_FOUND") {
			t.Fatalf("file was created at unrelated path %s outside jail; this should not happen", unrelatedDir)
		}

		// Verify the file WAS created at the jailed rewrite path, proving
		// the copyfile request actually succeeded rather than silently failing.
		jailedMarker := jailedPath + "/marker.txt"
		stdout, _, execErr = ExecOnHost(testHost, "cat "+jailedMarker+" 2>/dev/null || echo 'FILE_NOT_FOUND'")
		if execErr != nil {
			t.Fatal("exec failed:", execErr)
		}
		if strings.Contains(stdout, "FILE_NOT_FOUND") {
			t.Fatalf("marker file was not created at jailed path %s; copyfile may have silently failed", jailedMarker)
		}
	})
}
