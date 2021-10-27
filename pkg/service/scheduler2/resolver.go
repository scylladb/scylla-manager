// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"strings"

	"github.com/derekparker/trie"
	"github.com/scylladb/scylla-manager/pkg/util/uuid"
)

const (
	taskNamePfx = rune('_')
	taskNameSep = rune(0x1)
)

func toStr(r rune) string {
	return string([]rune{r})
}

type extTaskID struct {
	ClusterID uuid.UUID
	TaskID    uuid.UUID
	TaskName  string
}

func (eid extTaskID) key() string {
	return strings.ToLower(eid.TaskID.String())
}

func (eid extTaskID) nameKey() string {
	return strings.ToLower(toStr(taskNamePfx) + eid.TaskName + toStr(taskNameSep) + eid.ClusterID.String())
}

type resolver struct {
	cache *trie.Trie
}

func newResolver() resolver {
	return resolver{
		cache: trie.New(),
	}
}

func (r resolver) Put(eid extTaskID) {
	// Remove old name node
	node, ok := r.cache.Find(eid.key())
	if ok {
		old := node.Meta().(extTaskID)
		if old.TaskName != "" {
			r.cache.Remove(old.nameKey())
		}
	}

	r.cache.Add(eid.key(), eid)
	if eid.TaskName != "" {
		r.cache.Add(eid.nameKey(), eid)
	}
}

func (r resolver) Remove(taskID uuid.UUID) {
	node, ok := r.cache.Find(extTaskID{TaskID: taskID}.key())
	if !ok {
		return
	}
	eid := node.Meta().(extTaskID)

	r.cache.Remove(eid.key())
	if eid.TaskName != "" {
		r.cache.Remove(eid.nameKey())
	}
}

func (r resolver) Find(pre string) (extTaskID, bool) {
	// Find by ID
	if len(pre) >= 8 {
		if node := leafNode(findNode(r.cache.Root(), []rune(pre))); node != nil {
			return node.Meta().(extTaskID), true
		}
	}

	// Find by name
	node := findNode(r.cache.Root(), append([]rune{taskNamePfx}, []rune(pre)...))
	if sep := childNode(node, taskNameSep); sep != nil {
		node = leafNode(sep)
	} else {
		node = leafNode(node)
	}
	if node != nil {
		return node.Meta().(extTaskID), true
	}
	return extTaskID{}, false
}
