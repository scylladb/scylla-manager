package main

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/go-chi/render"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/config/agent"
	"github.com/scylladb/scylla-manager/v3/swagger/gen/agent/models"
)

type restoreHandler struct {
	config agent.Config
	logger log.Logger
}

func newRestoreHandler(c agent.Config) *restoreHandler {
	l, _ := c.MakeLogger()
	return &restoreHandler{config: c, logger: l}
}

func (h *restoreHandler) restore(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	h.logger.Info(ctx, "Restoring files")

	params := new(models.RestoreParams)
	dec := json.NewDecoder(r.Body)
	err := dec.Decode(params)
	if err != nil {
		h.respondWithInternalError(ctx, w, r, errors.Wrap(err, "decode params"))

		return
	}

	h.logger.Info(ctx, "Restore params",
		"keyspace", params.Keyspace,
		"table", params.Table,
		"version", params.Version,
		"files", params.Files,
	)

	uploadDir := uploadDataDir(h.config.Scylla.DataDirectory, params.Keyspace, params.Table, params.Version)
	absUploadDir, err := filepath.Abs(uploadDir)
	if err != nil {
		h.respondWithInternalError(ctx, w, r, errors.Wrap(err, "get upload directory absolute path"))

		return
	}

	h.logger.Info(ctx, "Upload data directory",
		"dir", absUploadDir,
	)

	//TODO - how to do this ownership in production? Should the manager be running as scylla user?
	for _, f := range params.Files {
		h.logger.Info(ctx, "Restore file",
			"file", f,
		)

		err = os.Chown(
			path.Join(absUploadDir, f),
			107,
			109,
		)

		if err != nil {
			h.respondWithInternalError(ctx, w, r, errors.Wrap(err, "chown file"))

			return
		}
	}

	if err := h.callLoadAndStream(ctx, params.Keyspace, params.Table); err != nil {
		h.respondWithInternalError(ctx, w, r, errors.Wrap(err, "call load and stream"))

		return
	}

	render.Status(r, http.StatusOK)
	render.Respond(w, r, "success")

	h.logger.Info(ctx, "Restore ended")
}

func (h *restoreHandler) callLoadAndStream(ctx context.Context, keyspace, table string) error {
	u := url.URL{
		Host:   h.APIAddr(),
		Scheme: "http",
		Path:   LoadAndStreamPath(keyspace),
	}

	q := u.Query()
	q.Add("cf", table)
	q.Add("load_and_stream", "true")
	q.Add("primary_replica_only", "true")
	u.RawQuery = q.Encode()

	c := http.DefaultClient
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), http.NoBody)
	if err != nil {
		return errors.Wrap(err, "create request")
	}

	resp, err := c.Do(req)
	if err != nil {
		return errors.Wrap(err, "send request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, err := io.ReadAll(resp.Body)
		if err != nil {
			return errors.Wrap(err, "read response body")
		}

		return errors.Errorf("status code %d: %s", resp.StatusCode, b)
	}

	return nil
}

func (h *restoreHandler) respondWithInternalError(ctx context.Context, w http.ResponseWriter, r *http.Request, err error) {
	render.Status(r, http.StatusInternalServerError)
	render.Respond(w, r, err)

	h.logger.Error(ctx, "Restore failed",
		"error", err,
	)
}

func (h *restoreHandler) APIAddr() string {
	return net.JoinHostPort(h.config.Scylla.APIAddress, h.config.Scylla.APIPort)
}

func uploadDataDir(dataDir, keyspace, table, version string) string {
	versionedTable := strings.Join([]string{table, version}, "-")
	return path.Join(
		dataDir,
		keyspace,
		versionedTable,
		"upload",
	)
}

func LoadAndStreamPath(keyspace string) string {
	return path.Join(
		"storage_service",
		"sstables",
		keyspace,
	)
}
