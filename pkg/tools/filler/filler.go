// Copyright (C) 2017 ScyllaDB

package filler

import (
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/go-log"
	"github.com/scylladb/mermaid/pkg/util/parallel"
	"go.uber.org/atomic"
)

// Filler puts a given amount of bytes into a single table named `data`.
type Filler struct {
	session  *gocql.Session
	size     *atomic.Int64
	bufSize  int64
	parallel int
	logger   log.Logger

	ctx context.Context
}

func NewFiller(session *gocql.Session, size, bufSize int64, parallel int, logger log.Logger) *Filler {
	return &Filler{
		session:  session,
		size:     atomic.NewInt64(size),
		bufSize:  bufSize,
		parallel: parallel,
		logger:   logger,
	}
}

// Run fills database.
func (f *Filler) Run(ctx context.Context) error {
	f.logger.Info(ctx, "Creating table", "table", "data")

	q := f.session.Query("CREATE TABLE IF NOT EXISTS data (id uuid PRIMARY KEY, data blob) " +
		"WITH compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb' : 500}")
	if err := q.Exec(); err != nil {
		return errors.Wrap(err, "create table")
	}
	q.Release()

	f.ctx = ctx

	if err := parallel.Run(f.parallel, parallel.NoLimit, f.fill); err != nil {
		return errors.Wrap(err, "fill data")
	}

	return nil
}

func (f *Filler) fill(i int) error {
	q := f.session.Query("INSERT INTO data (id, data) VALUES (uuid(), ?)")
	defer q.Release()

	data := make([]byte, f.bufSize)

	f.logger.Info(f.ctx, "Writing data", "worker", i)

	done := make(chan struct{})
	defer close(done)

	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()

		for {
			select {
			case <-done:
				return
			case <-t.C:
				f.logger.Info(f.ctx, "Remaining", "worker", i, "bytes", f.size.Load())
			}
		}
	}()

	for f.size.Load() > 0 {
		if err := f.ctx.Err(); err != nil {
			return err
		}

		rand.Read(data)

		if err := q.Bind(data).Exec(); err != nil {
			f.logger.Info(f.ctx, "DB error", "worker", i, "error", err)
		} else {
			f.size.Sub(f.bufSize)
		}
	}

	f.logger.Info(f.ctx, "Done writing data", "worker", i)

	return nil
}

// MultiFiller puts a given amount of bytes into many tables named `data_XXX`.
type MultiFiller struct {
	tables   int
	session  *gocql.Session
	size     *atomic.Int64
	bufSize  int64
	parallel int
	logger   log.Logger

	ctx context.Context
}

func NewMultiFiller(tables int, session *gocql.Session, size, bufSize int64, parallel int, logger log.Logger) *MultiFiller {
	return &MultiFiller{
		tables:   tables,
		session:  session,
		size:     atomic.NewInt64(size),
		bufSize:  bufSize,
		parallel: parallel,
		logger:   logger,
	}
}

// Run fills database.
func (f *MultiFiller) Run(ctx context.Context) error {
	f.ctx = ctx

	if err := parallel.Run(f.tables, runtime.NumCPU(), f.create); err != nil {
		return errors.Wrap(err, "create table")
	}

	if err := parallel.Run(f.parallel, parallel.NoLimit, f.fill); err != nil {
		return errors.Wrap(err, "fill data")
	}

	return nil
}

func (f *MultiFiller) create(i int) error {
	f.logger.Info(f.ctx, "Creating table", "table", fmt.Sprintf("data_%d", i))

	q := f.session.Query(fmt.Sprintf("CREATE TABLE IF NOT EXISTS data_%d (id uuid PRIMARY KEY, data blob) "+
		"WITH compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb' : 500}", i))
	defer q.Release()

	return q.Exec()
}

func (f *MultiFiller) fill(i int) error {
	var (
		qs   []*gocql.Query
		data = make([]byte, f.bufSize)
	)

	block := f.tables / f.parallel
	start := i * block
	end := start + block

	for t := start; t < end; t++ {
		q := f.session.Query(fmt.Sprintf("INSERT INTO data_%d (id, data) VALUES (uuid(), ?)", t))
		q.Bind(data)
		qs = append(qs, q)
	}

	f.logger.Info(f.ctx, "Writing data", "worker", i, "start", start, "end", end)

	done := make(chan struct{})
	defer close(done)

	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()

		for {
			select {
			case <-done:
				return
			case <-t.C:
				f.logger.Info(f.ctx, "Remaining", "worker", i, "bytes", f.size.Load())
			}
		}
	}()

	pos := 0
	for f.size.Load() > 0 {
		if err := f.ctx.Err(); err != nil {
			return err
		}

		rand.Read(data)

		if err := qs[pos].Bind(data).Exec(); err != nil {
			f.logger.Info(f.ctx, "DB error", "worker", i, "error", err)
		} else {
			f.size.Sub(f.bufSize)
		}

		pos = (pos + 1) % len(qs)
	}

	f.logger.Info(f.ctx, "Done writing data", "worker", i)

	return nil
}
