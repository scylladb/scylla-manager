// Copyright (C) 2017 ScyllaDB

package scheduler

import (
	"fmt"
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

type taskInfo struct {
	ClusterID uuid.UUID
	TaskType  TaskType
	TaskID    uuid.UUID
	TaskName  string
}

func newTaskInfoFromTask(t *Task) taskInfo {
	return taskInfo{
		ClusterID: t.ClusterID,
		TaskType:  t.Type,
		TaskID:    t.ID,
		TaskName:  t.Name,
	}
}

func (ti taskInfo) key() string {
	return strings.ToLower(ti.TaskID.String())
}

func (ti taskInfo) nameKey() string {
	return strings.ToLower(toStr(taskNamePfx) + ti.TaskName + toStr(taskNameSep) + ti.ClusterID.String())
}

func (ti taskInfo) String() string {
	return fmt.Sprintf("%s/%s", ti.TaskType, ti.TaskID)
}

type resolver struct {
	cache *trie.Trie
}

func newResolver() resolver {
	return resolver{
		cache: trie.New(),
	}
}

func (r resolver) Put(ti taskInfo) {
	// Remove old name node
	node, ok := r.cache.Find(ti.key())
	if ok {
		old := node.Meta().(taskInfo)
		if old.TaskName != "" {
			r.cache.Remove(old.nameKey())
		}
	}

	r.cache.Add(ti.key(), ti)
	if ti.TaskName != "" {
		r.cache.Add(ti.nameKey(), ti)
	}
}

func (r resolver) Remove(taskID uuid.UUID) {
	node, ok := r.cache.Find(taskInfo{TaskID: taskID}.key())
	if !ok {
		return
	}
	ti := node.Meta().(taskInfo)

	r.cache.Remove(ti.key())
	if ti.TaskName != "" {
		r.cache.Remove(ti.nameKey())
	}
}

func (r resolver) Find(pre string) (taskInfo, bool) {
	// Find by ID
	if node := leafNode(findNode(r.cache.Root(), []rune(pre))); node != nil {
		return node.Meta().(taskInfo), true
	}

	// Find by name
	node := findNode(r.cache.Root(), append([]rune{taskNamePfx}, []rune(pre)...))
	if sep := childNode(node, taskNameSep); sep != nil {
		node = leafNode(sep)
	} else {
		node = leafNode(node)
	}
	if node != nil {
		return node.Meta().(taskInfo), true
	}
	return taskInfo{}, false
}
