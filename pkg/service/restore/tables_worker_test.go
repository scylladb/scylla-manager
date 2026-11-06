// Copyright (C) 2026 ScyllaDB

package restore

import (
	"context"
	"errors"
	"testing"

	"github.com/scylladb/go-log"
	"github.com/scylladb/scylla-manager/v3/pkg/scyllaclient"
)

type fakeBalancingRingDescriber struct {
	scyllaclient.RingDescriber

	tabletKs   map[string]bool
	disableErr error
	calls      []bool
	ctxErrs    []error
}

func (f *fakeBalancingRingDescriber) IsTabletKeyspace(keyspace string) bool {
	return f.tabletKs[keyspace]
}

func (f *fakeBalancingRingDescriber) ControlTabletLoadBalancing(ctx context.Context, enabled bool) error {
	f.calls = append(f.calls, enabled)
	f.ctxErrs = append(f.ctxErrs, ctx.Err())
	if !enabled {
		return f.disableErr
	}
	return nil
}

func TestDisableTabletBalancing(t *testing.T) {
	rdw := func(ks string, sstables int) RemoteDirWorkload {
		return RemoteDirWorkload{
			TableName: TableName{Keyspace: ks, Table: "t"},
			SSTables:  make([]RemoteSSTable, sstables),
		}
	}
	disableErr := errors.New("disable failed")

	testCases := []struct {
		name         string
		remoteDir    []RemoteDirWorkload
		disableErr   error
		wantDisabled bool
		wantErr      bool
	}{
		{name: "no work"},
		{name: "vnode keyspace only", remoteDir: []RemoteDirWorkload{rdw("vnode", 2)}},
		{name: "tablet keyspace already restored", remoteDir: []RemoteDirWorkload{rdw("tablet", 0)}},
		{name: "tablet keyspace", remoteDir: []RemoteDirWorkload{rdw("vnode", 1), rdw("tablet", 1), rdw("tablet", 3)}, wantDisabled: true},
		{name: "disable fails", remoteDir: []RemoteDirWorkload{rdw("tablet", 1)}, disableErr: disableErr, wantDisabled: true, wantErr: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			rd := &fakeBalancingRingDescriber{
				tabletKs:   map[string]bool{"tablet": true},
				disableErr: tc.disableErr,
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			w := &tablesWorker{worker: worker{logger: log.NopLogger}}
			reenable, err := w.disableTabletBalancing(ctx, rd, tc.remoteDir)
			if (err != nil) != tc.wantErr || (tc.wantErr && !errors.Is(err, disableErr)) {
				t.Fatalf("disableTabletBalancing() error = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantDisabled {
				if err := reenable(); err != nil || len(rd.calls) != 0 {
					t.Fatalf("expected balancing untouched, got err=%v calls=%v", err, rd.calls)
				}
				return
			}
			if len(rd.calls) != 1 || rd.calls[0] {
				t.Fatalf("expected a single disable call, got calls=%v", rd.calls)
			}

			// Simulate pause: re-enable must not use the canceled restore ctx.
			cancel()
			if err := reenable(); err != nil {
				t.Fatalf("reenable() error = %v", err)
			}
			if len(rd.calls) != 2 || !rd.calls[1] || rd.ctxErrs[1] != nil {
				t.Fatalf("expected re-enable with live ctx, got calls=%v ctxErrs=%v", rd.calls, rd.ctxErrs)
			}
		})
	}
}
