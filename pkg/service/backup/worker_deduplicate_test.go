// Copyright (C) 2026 ScyllaDB

package backup

import (
	"context"
	"errors"
	"math"
	"path"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/scylladb/scylla-manager/backupspec"
)

const (
	uuidSSTable  = "me-3g7k_098r_4wtqo2asamoc1i8h9n-big-COMPONENT"
	uuidSSTable2 = "me-3g7k_098r_50i0g2asamoc1i8h9n-big-COMPONENT"
	intSSTable   = "me-7-big-COMPONENT"
	intSSTable2  = "me-8-big-COMPONENT"
)

// versionSnapshotTag is a snapshot tag used as a versioned file suffix.
var versionSnapshotTag = backupspec.SnapshotTagAt(time.Date(2026, 1, 15, 10, 30, 0, 0, time.UTC))

// bundle creates sstable bundle files of the given size
// by substituting COMPONENT in the name template.
func bundle(name string, size int64, components ...string) []fileInfo {
	out := make([]fileInfo, 0, len(components))
	for _, c := range components {
		out = append(out, fileInfo{
			Name: component(name, c),
			Size: size,
		})
	}
	return out
}

// component returns a single sstable bundle file name.
func component(name, c string) string {
	return strings.Replace(name, "COMPONENT", c, 1)
}

func fileNames(files []fileInfo) []string {
	out := make([]string, 0, len(files))
	for _, f := range files {
		out = append(out, f.Name)
	}
	slices.Sort(out)
	return out
}

const (
	testLocalDir  = "/var/lib/scylla/data/ks/t/snapshots/sm_tag"
	testRemoteDir = "s3:bucket/backup/sst/cluster/dc/node/ks/t/version"
)

// digestCat returns a catFunc serving the given path to content mapping.
func digestCat(t *testing.T, digests map[string]string) catFunc {
	t.Helper()
	return func(_ context.Context, remotePath string) ([]byte, error) {
		content, ok := digests[remotePath]
		if !ok {
			t.Errorf("unexpected cat of %s", remotePath)
		}
		return []byte(content), nil
	}
}

// runDeduplicator feeds local and remote files to the deduplicator
// and returns its result.
func runDeduplicator(t *testing.T, cat catFunc, local, remote []fileInfo) (deduplicated []fileInfo, versionedCnt int, err error) {
	t.Helper()
	d := newSSTableDeduplicator(cat, testRemoteDir, testLocalDir)
	for _, f := range local {
		if err := d.addLocal(f.Name, f.Size); err != nil {
			t.Fatalf("add local %s: %s", f.Name, err)
		}
	}
	d.finalizeLocal()
	for _, f := range remote {
		if err := d.addRemote(f.Name, f.Size); err != nil {
			t.Fatalf("add remote %s: %s", f.Name, err)
		}
	}
	return d.result(t.Context())
}

func TestSSTableDeduplicator(t *testing.T) {
	t.Parallel()

	// Digests of the intSSTable and intSSTable2 bundles.
	localDigest := path.Join(testLocalDir, component(intSSTable, "Digest.crc32"))
	remoteDigest := path.Join(testRemoteDir, component(intSSTable, "Digest.crc32"))
	localDigest2 := path.Join(testLocalDir, component(intSSTable2, "Digest.crc32"))
	remoteDigest2 := path.Join(testRemoteDir, component(intSSTable2, "Digest.crc32"))

	testCases := []struct {
		name   string
		local  []fileInfo
		remote []fileInfo
		// digests maps file path to the content served by catFunc.
		// Reading a path outside of this map fails the test.
		digests map[string]string
		// expected are the file names which can be skipped during upload.
		expected []string
		// expectedVersioned is the amount of files which have a remote
		// counterpart bundle, but still can't be deduplicated.
		expectedVersioned int
	}{
		{
			name:  "scylla manifest is ignored",
			local: bundle(uuidSSTable, 10, "Data.db", "Index.db"),
			remote: append(bundle(uuidSSTable, 10, "Data.db", "Index.db"),
				fileInfo{Name: backupspec.ScyllaManifest, Size: 10}),
			expected: fileNames(bundle(uuidSSTable, 10, "Data.db", "Index.db")),
		},
		{
			name:     "identical uuid bundle is deduplicated",
			local:    bundle(uuidSSTable, 10, "Data.db", "Index.db", "TOC.txt"),
			remote:   bundle(uuidSSTable, 10, "Data.db", "Index.db", "TOC.txt"),
			expected: fileNames(bundle(uuidSSTable, 10, "Data.db", "Index.db", "TOC.txt")),
		},
		{
			name:   "uuid bundle with no remote counterpart",
			local:  bundle(uuidSSTable, 10, "Data.db", "Index.db"),
			remote: bundle(uuidSSTable2, 10, "Data.db", "Index.db"),
		},
		{
			name:   "uuid bundle with different component size",
			local:  bundle(uuidSSTable, 10, "Data.db", "Index.db"),
			remote: append(bundle(uuidSSTable, 10, "Data.db"), bundle(uuidSSTable, 11, "Index.db")...),
		},
		{
			name:   "uuid bundle with missing remote component",
			local:  bundle(uuidSSTable, 10, "Data.db", "Index.db", "TOC.txt"),
			remote: bundle(uuidSSTable, 10, "Data.db", "Index.db"),
		},
		{
			name:   "uuid bundle with additional remote component",
			local:  bundle(uuidSSTable, 10, "Data.db", "Index.db"),
			remote: bundle(uuidSSTable, 10, "Data.db", "Index.db", "TOC.txt"),
		},
		{
			name:  "remote files with no local counterpart are ignored",
			local: bundle(uuidSSTable, 10, "Data.db", "Index.db"),
			remote: slices.Concat(
				bundle(uuidSSTable, 10, "Data.db", "Index.db"),
				bundle(uuidSSTable2, 10, "Data.db", "Index.db"),
			),
			expected: fileNames(bundle(uuidSSTable, 10, "Data.db", "Index.db")),
		},
		{
			name: "only matching uuid bundles out of many",
			local: slices.Concat(
				bundle(uuidSSTable, 10, "Data.db", "Index.db"),
				bundle(uuidSSTable2, 10, "Data.db", "Index.db"),
			),
			remote: slices.Concat(
				bundle(uuidSSTable, 10, "Data.db", "Index.db"),
				bundle(uuidSSTable2, 11, "Data.db", "Index.db"),
			),
			expected: fileNames(bundle(uuidSSTable, 10, "Data.db", "Index.db")),
		},
		{
			name:   "int bundle with the same digest is deduplicated",
			local:  bundle(intSSTable, 10, "Data.db", "Digest.crc32"),
			remote: bundle(intSSTable, 10, "Data.db", "Digest.crc32"),
			digests: map[string]string{
				localDigest:  "abc",
				remoteDigest: "abc",
			},
			expected: fileNames(bundle(intSSTable, 10, "Data.db", "Digest.crc32")),
		},
		{
			name:   "int bundle with different digest is versioned",
			local:  bundle(intSSTable, 10, "Data.db", "Digest.crc32"),
			remote: bundle(intSSTable, 10, "Data.db", "Digest.crc32"),
			digests: map[string]string{
				localDigest:  "abc",
				remoteDigest: "cba",
			},
			expectedVersioned: 2,
		},
		{
			name:              "int bundle with no digest component is versioned",
			local:             bundle(intSSTable, 10, "Data.db", "Index.db"),
			remote:            bundle(intSSTable, 10, "Data.db", "Index.db"),
			expectedVersioned: 2,
		},
		{
			name:              "int bundle with different component size is versioned",
			local:             bundle(intSSTable, 10, "Data.db", "Digest.crc32"),
			remote:            bundle(intSSTable, 11, "Data.db", "Digest.crc32"),
			expectedVersioned: 2,
		},
		{
			name:              "int bundle with additional remote component is versioned",
			local:             bundle(intSSTable, 10, "Data.db", "Digest.crc32"),
			remote:            bundle(intSSTable, 10, "Data.db", "Digest.crc32", "TOC.txt"),
			expectedVersioned: 2,
		},
		{
			name:   "int bundle with no remote counterpart is not versioned",
			local:  bundle(intSSTable, 10, "Data.db", "Digest.crc32"),
			remote: bundle(intSSTable2, 10, "Data.db", "Digest.crc32"),
		},
		{
			name:  "versioned remote file is ignored",
			local: bundle(intSSTable, 10, "Data.db", "Digest.crc32"),
			remote: append(bundle(intSSTable, 10, "Data.db", "Digest.crc32"),
				fileInfo{Name: component(intSSTable, "Data.db") + "." + versionSnapshotTag, Size: 11}),
			digests: map[string]string{
				localDigest:  "abc",
				remoteDigest: "abc",
			},
			expected: fileNames(bundle(intSSTable, 10, "Data.db", "Digest.crc32")),
		},
		{
			name: "int and uuid bundles at once",
			local: slices.Concat(
				bundle(uuidSSTable, 10, "Data.db", "Index.db"),
				bundle(intSSTable, 10, "Data.db", "Digest.crc32"),
				bundle(intSSTable2, 10, "Data.db", "Digest.crc32"),
			),
			remote: slices.Concat(
				bundle(uuidSSTable, 10, "Data.db", "Index.db"),
				bundle(intSSTable, 10, "Data.db", "Digest.crc32"),
				bundle(intSSTable2, 10, "Data.db", "Digest.crc32"),
			),
			digests: map[string]string{
				localDigest:   "abc",
				remoteDigest:  "abc",
				localDigest2:  "abc",
				remoteDigest2: "cba",
			},
			expected: fileNames(slices.Concat(
				bundle(uuidSSTable, 10, "Data.db", "Index.db"),
				bundle(intSSTable, 10, "Data.db", "Digest.crc32"),
			)),
			expectedVersioned: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, gotVersioned, err := runDeduplicator(t, digestCat(t, tc.digests), tc.local, tc.remote)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(fileNames(got), tc.expected) {
				t.Errorf("deduplicated got %v, expected %v", fileNames(got), tc.expected)
			}
			if gotVersioned != tc.expectedVersioned {
				t.Errorf("versionedCnt got %d, expected %d", gotVersioned, tc.expectedVersioned)
			}
		})
	}
}

func TestSSTableDeduplicatorDigestError(t *testing.T) {
	t.Parallel()

	catErr := errors.New("cat failed")
	cat := func(_ context.Context, _ string) ([]byte, error) {
		return nil, catErr
	}

	local := bundle(intSSTable, 10, "Data.db", "Digest.crc32")
	_, _, err := runDeduplicator(t, cat, local, local)
	if !errors.Is(err, catErr) {
		t.Fatalf("got %v, expected %v", err, catErr)
	}
}

// TestSSTableDeduplicatorKeepsOnlyLocalFiles validates that remote files
// with no local counterpart are not stored by the deduplicator.
func TestSSTableDeduplicatorKeepsOnlyLocalFiles(t *testing.T) {
	t.Parallel()

	local := bundle(uuidSSTable, 10, "Data.db", "Index.db")
	d := newSSTableDeduplicator(digestCat(t, nil), testRemoteDir, testLocalDir)
	for _, f := range local {
		if err := d.addLocal(f.Name, f.Size); err != nil {
			t.Fatal(err)
		}
	}
	d.finalizeLocal()

	// Feed a lot of remote files belonging to other generations.
	const remoteBundles = 10000
	for i := range remoteBundles {
		for _, f := range bundle(sstableName(i), 10, "Data.db", "Index.db") {
			if err := d.addRemote(f.Name, f.Size); err != nil {
				t.Fatal(err)
			}
		}
	}

	if len(d.local) != 1 {
		t.Fatalf("deduplicator tracks %d bundles, expected 1", len(d.local))
	}
	for id, b := range d.local {
		if len(b.files) != len(local) {
			t.Fatalf("bundle %s tracks %d files, expected %d", id, len(b.files), len(local))
		}
		if b.remoteCnt != 0 {
			t.Fatalf("bundle %s observed %d remote files, expected 0", id, b.remoteCnt)
		}
	}
}

// sstableName returns a name template of the i-th unique int ID sstable.
func sstableName(i int) string {
	return "me-" + strconv.Itoa(i+1000) + "-big-COMPONENT"
}

// uuidSSTableName returns a name template of the i-th unique sstable.UUID sstable.
func uuidSSTableName(i int) string {
	msb := strconv.FormatInt(int64(i), 36)
	return "me-3g7k_098r_4wtqo" + strings.Repeat("0", 13-len(msb)) + msb + "-big-COMPONENT"
}

// deduplicatorRetainedHeap returns the amount of heap still held by the deduplicator
// after processing localBundles local and remoteBundles remote sstables.
// File names are generated on the fly, so that the names of the remote
// sstables which the deduplicator didn't store can be garbage collected.
// This way the result consists of what the deduplicator retains and
// nothing else.
func deduplicatorRetainedHeap(tb testing.TB, localBundles, remoteBundles int) uint64 {
	tb.Helper()
	before := heapAlloc()

	d := newSSTableDeduplicator(unexpectedCat(tb), testRemoteDir, testLocalDir)
	for i := range localBundles {
		for _, f := range bundle(uuidSSTableName(i), 10, "Data.db", "Index.db") {
			if err := d.addLocal(f.Name, f.Size); err != nil {
				tb.Fatal(err)
			}
		}
	}
	d.finalizeLocal()
	for i := range remoteBundles {
		for _, f := range bundle(uuidSSTableName(i), 10, "Data.db", "Index.db") {
			if err := d.addRemote(f.Name, f.Size); err != nil {
				tb.Fatal(err)
			}
		}
	}

	after := heapAlloc()
	runtime.KeepAlive(d)
	if after < before {
		return 0
	}
	return after - before
}

// heapAlloc returns the amount of allocated heap after garbage collection.
// GC is called twice, as a single run can leave the memory freed
// by the finalizers uncollected.
func heapAlloc() uint64 {
	runtime.GC()
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// unexpectedCat returns a catFunc failing on any file read.
func unexpectedCat(tb testing.TB) catFunc {
	return func(_ context.Context, remotePath string) ([]byte, error) {
		tb.Errorf("unexpected cat of %s", remotePath)
		return nil, nil
	}
}

// BenchmarkSSTableDeduplicator measures the heap retained by the deduplicator
// after processing a whole remote sstable dir against a fixed amount of local
// sstables. It must stay flat as the amount of remote sstables grows, since
// only the local ones are stored.
// Every iteration performs a full pass over the remote dir and the smallest
// result is reported, as unrelated live objects can only inflate the reading.
// Timing is not reported, as repeating the pass doesn't change the
// retained heap.
func BenchmarkSSTableDeduplicator(b *testing.B) {
	const localBundles = 1000

	for _, remoteBundles := range []int{localBundles, 100 * localBundles} {
		b.Run("remote="+strconv.Itoa(remoteBundles), func(b *testing.B) {
			retained := uint64(math.MaxUint64)
			for b.Loop() {
				retained = min(retained, deduplicatorRetainedHeap(b, localBundles, remoteBundles))
			}
			b.ReportMetric(0, "ns/op")
			b.ReportMetric(float64(retained), "retained-B")
		})
	}
}
