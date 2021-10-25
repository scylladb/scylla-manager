// Copyright (C) 2017 ScyllaDB

package restapi

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pkg/errors"
	"github.com/scylladb/scylla-manager/pkg/httpexec"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
	"github.com/scylladb/scylla-manager/pkg/service/cluster"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
)

func (h clusterHandler) execFilter(r *http.Request) (cluster.ExecFilter, error) {
	if err := r.ParseForm(); err != nil {
		return cluster.ExecFilter{}, err
	}

	f := cluster.ExecFilter{
		Hosts:      r.Form["host"],
		Datacenter: r.Form["datacenter"],
		State:      scyllaclient.NodeState(r.FormValue("state")),
		Status:     scyllaclient.NodeStatus(r.FormValue("status")),
	}

	if l := r.FormValue("limit"); l != "" {
		v, err := strconv.ParseInt(l, 10, 32)
		if err != nil {
			return cluster.ExecFilter{}, errors.Wrap(err, "limit")
		}
		f.Limit = int(v)
	}

	return f, nil
}

const (
	// Time allowed to write a message to the peer.
	writeWait = 10 * time.Second

	// Maximum message size allowed from peer.
	maxMessageSize = 8192

	// Time allowed to read the next pong message from the peer.
	pongWait = 60 * time.Second

	// Send pings to peer with this period. Must be less than pongWait.
	pingPeriod = (pongWait * 9) / 10

	// Time to wait before force close on connection.
	closeGracePeriod = 10 * time.Second
)

func (h clusterHandler) exec(w http.ResponseWriter, r *http.Request) {
	c := mustClusterFromCtx(r)

	if r.ContentLength > maxMessageSize || r.ContentLength == 0 {
		respondBadRequest(w, r, errors.Errorf("Max message size is %d", maxMessageSize))
		return
	}

	f, err := h.execFilter(r)
	if err != nil {
		respondBadRequest(w, r, err)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		respondBadRequest(w, r, err)
		return
	}

	var cw io.Writer = w
	if f, ok := w.(http.Flusher); ok {
		cw = httpexec.NewFlusher(w, f, '\n')
	}
	if err := h.svc.Exec(r.Context(), c.ID, body, cw, f); err != nil {
		fmt.Println("Exec error", err)
	}
}

func pumpStdin(ctx context.Context, ws *websocket.Conn, f func(ctx context.Context, message []byte) error) {
	defer ws.Close()
	ws.SetReadLimit(maxMessageSize)
	ws.SetReadDeadline(timeutc.Now().Add(pongWait))
	ws.SetPongHandler(func(string) error { ws.SetReadDeadline(timeutc.Now().Add(pongWait)); return nil })

	var (
		execCtx context.Context
		cancel  context.CancelFunc
		idx     uint
	)
	for {
		_, message, err := ws.ReadMessage()
		if err != nil {
			break
		}
		if cancel != nil {
			cancel()
		}
		execCtx, cancel = context.WithCancel(ctx)
		go func(idx uint) {
			if err := f(execCtx, message); err != nil {
				log.Println("processing error", err)
			}
			ws.SetWriteDeadline(timeutc.Now().Add(writeWait))
			if err := ws.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("___SM_COMMAND_DONE:%d", idx))); err != nil {
				log.Println("command end", err)
			}
		}(idx)
		idx += 1
	}
}

func pumpStdout(ws *websocket.Conn, r io.Reader, done chan struct{}) {
	s := bufio.NewScanner(r)
	for s.Scan() {
		ws.SetWriteDeadline(timeutc.Now().Add(writeWait))
		if err := ws.WriteMessage(websocket.TextMessage, s.Bytes()); err != nil {
			ws.Close()
			break
		}
	}
	if s.Err() != nil {
		log.Println("scan:", s.Err())
	}
	close(done)

	ws.SetWriteDeadline(timeutc.Now().Add(writeWait))
	ws.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	time.Sleep(closeGracePeriod)
	ws.Close()
}

func ping(ws *websocket.Conn, done chan struct{}) {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := ws.WriteControl(websocket.PingMessage, []byte{}, timeutc.Now().Add(writeWait)); err != nil {
				log.Println("ping:", err)
			}
		case <-done:
			return
		}
	}
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func internalError(ws *websocket.Conn, msg string, err error) {
	log.Println(msg, err)
	ws.WriteMessage(websocket.TextMessage, []byte("Internal server error."))
}

func (h clusterHandler) execWS(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		respondBadRequest(w, r, err)
		return
	}
	defer ws.Close()

	c := mustClusterFromCtx(r)
	f, err := h.execFilter(r)
	if err != nil {
		internalError(ws, "stdout:", err)
		return
	}

	outr, outw := io.Pipe()
	defer outr.Close()
	defer outw.Close()

	stdoutDone := make(chan struct{})
	go pumpStdout(ws, outr, stdoutDone)
	go ping(ws, stdoutDone)

	pumpStdin(r.Context(), ws, func(ctx context.Context, message []byte) error {
		return h.svc.Exec(ctx, c.ID, message, outw, f)
	})
}
