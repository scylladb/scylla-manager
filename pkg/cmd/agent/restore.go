package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"

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

// TODO - properly handle errors
func (h *restoreHandler) restore(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	h.logger.Info(ctx, "Begin Restore")

	params := new(models.RestoreParams)
	dec := json.NewDecoder(r.Body)
	err := dec.Decode(params)
	if err != nil {
		render.Status(r, http.StatusInternalServerError)
		render.Respond(w, r, errors.Wrap(err, "decode params"))
	}

	uploadDir := fmt.Sprintf(
		"%s/%s/%s-%s/upload",
		h.config.Scylla.DataDirectory,
		params.Keyspace,
		params.Table,
		params.Version,
	)

	absDataDir, err := filepath.Abs(uploadDir)
	if err != nil {
		panic(errors.Wrap(err, "get upload directory absolute path"))
	}

	h.logger.Info(ctx, "Restoring Files",
		"uploadDir", uploadDir,
		"files", params.Files,
	)

	//TODO: why is it even necessary?
	//TODO - how to do this ownership in production? Should the manager be running as scylla user?
	for _, f := range params.Files {
		os.Chown(
			path.Join(absDataDir, f),
			107,
			109,
		)
	}

	if err := h.callSSTables(ctx, params.Keyspace, params.Table); err != nil {
		// TODO - err
		panic(err)
	}

	render.Respond(w, r, "success")
}

func (h *restoreHandler) callSSTables(ctx context.Context, keyspace, table string) error {
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
		return err
	}

	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		buf, err := io.ReadAll(resp.Body)
		if err != nil {
			return err
		}

		return errors.Errorf("status code %d: %s", 200, buf)
	}

	return nil
}

func (h *restoreHandler) APIAddr() string {
	return net.JoinHostPort(h.config.Scylla.APIAddress, h.config.Scylla.APIPort)
}

func LoadAndStreamPath(keyspace string) string {
	return path.Join(
		"/storage_service",
		"sstables",
		keyspace,
	)
}
