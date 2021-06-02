package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/pkg/service/backup/backupspec"
	"github.com/scylladb/scylla-manager/pkg/util/timeutc"
)

type NodeInfo struct {
	Name  string `json:"name"`
	Dsize int64  `json:"dsize"`
	Mtime int64  `json:"mtime"`
}

func main() {
	dir := flag.String("dir", "", "path to directory containing ncdu.out and meta dir")
	flag.Parse()

	manifests, files, sizes, mtimes := loadFiles(path.Join(*dir, "ncdu.out"))

	expectedFiles := strset.New()
	for _, p := range manifests {
		loadManifestInto(*dir, p, expectedFiles)
	}
	sort.Strings(files)

	deletes, err := os.Create(path.Join(*dir, "deletes-to-exec"))
	if err != nil {
		log.Fatal("Create()", err)
	}
	dw := bufio.NewWriter(deletes)
	defer func() {
		if err := dw.Flush(); err != nil {
			panic("Flush")
		}
		if err := deletes.Close(); err != nil {
			panic("Close")
		}
	}()

	summary, err := os.Create(path.Join(*dir, "summary"))
	if err != nil {
		log.Fatal("Create()", err)
	}
	sw := bufio.NewWriter(summary)
	defer func() {
		if err := sw.Flush(); err != nil {
			panic("Flush")
		}
		if err := summary.Close(); err != nil {
			panic("Close")
		}
	}()

	csv, err := os.Create(path.Join(*dir, "report.csv"))
	if err != nil {
		log.Fatal("Create()", err)
	}
	w := bufio.NewWriter(csv)
	defer func() {
		if err := w.Flush(); err != nil {
			panic("Flush")
		}
		if err := csv.Close(); err != nil {
			panic("Close")
		}
	}()

	var (
		orphaned      int64
		orphanedBytes int64
		common        int64
		maxTime       int64
	)
	for i, f := range files {
		if !expectedFiles.Has(f) {
			// Write to delete query
			if orphaned%1000 == 0 {
				if orphaned > 0 {
					fmt.Fprintln(dw, "]}")
				}
				fmt.Fprintf(dw, `{"Objects":[{"Key":"%s"}`, f)
			} else {
				fmt.Fprintf(dw, `,{"Key":"%s"}`, f)
			}

			// Write to CSV report
			orphaned++
			orphanedBytes += sizes[i]
			if mtimes[i] > maxTime {
				maxTime = mtimes[i]
			}
			fmt.Fprintf(w, "%s;%d;%d\n", f, sizes[i], mtimes[i])
		} else {
			common++
		}
	}
	if orphaned > 0 {
		fmt.Fprintln(dw, "]}")
	}

	out := io.MultiWriter(sw, os.Stdout)

	fmt.Fprintln(out, "Expected:\t\t", expectedFiles.Size())
	fmt.Fprintln(out, "In ncdu file:\t\t", len(files))
	fmt.Fprintln(out, "Common:\t\t", common)
	fmt.Fprintln(out, "Orphaned:\t\t", orphaned)
	fmt.Fprintln(out, "Orphaned bytes:\t\t", orphanedBytes)
	fmt.Fprintln(out, "Max orphaned time:\t\t", time.Unix(maxTime, 0))
}

func loadManifestInto(dir, fileName string, files *strset.Set) {
	f, err := os.Open(path.Join(dir, strings.Join(strings.Split(fileName, "/")[1:], "/")))
	if err != nil {
		log.Fatal("Open()", err)
	}
	defer f.Close()

	var m backupspec.RemoteManifest
	if err := m.ParsePath(fileName); err != nil {
		log.Fatal(err)
	}

	c := backupspec.ManifestContent{}
	if err := c.Read(f); err != nil {
		log.Fatal("Read()", err)
	}

	var filesCount int64
	for _, fi := range c.Index {
		baseDir := backupspec.RemoteSSTableVersionDir(m.ClusterID, m.DC, m.NodeID, fi.Keyspace, fi.Table, fi.Version)
		for _, f := range fi.Files {
			files.Add(path.Join(baseDir, f))
			filesCount++
		}
	}

	log.Printf("Loaded Manifest %s with %d files", fileName, filesCount)
}

var threshold = timeutc.Now().AddDate(0, -1, 0)

func loadFiles(fileName string) (manifests, files []string, sizes, mtimes []int64) {
	log.Printf("Loading %s", fileName)

	f, err := os.Open(fileName)
	if err != nil {
		log.Fatal("Open()", err)
	}
	defer f.Close()

	var dirStack []string

	s := bufio.NewScanner(bufio.NewReader(f))
	s.Scan()
	for s.Scan() {
		v := s.Text()

		switch {
		case strings.HasPrefix(v, `[{"name"`):
			d := NodeInfo{}
			if err := json.Unmarshal([]byte(v[1:len(v)-1]), &d); err != nil {
				log.Fatal("json.Unmarshal", err, v)
			}
			dirStack = append(dirStack, d.Name)
		case strings.HasPrefix(v, `{`):
			v = v[0 : len(v)-1]

			var popDir int
			for v[len(v)-1] == ']' {
				popDir++
				v = v[:len(v)-1]
			}

			n := NodeInfo{}
			if err := json.Unmarshal([]byte(v), &n); err != nil {
				log.Fatal("json.Unmarshal", err, v)
			}

			switch {
			case strings.HasSuffix(n.Name, ".version"):
			// Ignore
			case strings.HasSuffix(n.Name, "manifest.json.gz"):
				if time.Unix(n.Mtime, 0).Before(threshold) {
					log.Printf("Detected super old manifest - will be removed %s", n.Name)
					files = append(files, strings.Join(dirStack[1:], "/")+"/"+n.Name)
					sizes = append(sizes, n.Dsize)
					mtimes = append(mtimes, n.Mtime)
				} else {
					manifests = append(manifests, strings.Join(dirStack[1:], "/")+"/"+n.Name)
				}
			default:
				files = append(files, strings.Join(dirStack[1:], "/")+"/"+n.Name)
				sizes = append(sizes, n.Dsize)
				mtimes = append(mtimes, n.Mtime)
			}

			if popDir > 0 {
				dirStack = dirStack[:len(dirStack)-popDir]
			}
		default:
			log.Fatal("Unsupported line", v)
		}
	}

	return manifests, files, sizes, mtimes
}
