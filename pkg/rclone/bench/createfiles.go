// Copyright (C) 2017 ScyllaDB

package bench

import (
	"fmt"
	"os"
	"path"
)

// CreateFiles creates fileCount number of files with sizeMb in megabytes at
// scenarioPath.
func CreateFiles(dir string, sizeMB, fileCount int) error {
	size := int64(sizeMB * 1024 * 1024)
	filePrefix := fmt.Sprintf("scenario%dx%dmb", fileCount, sizeMB)

	err := os.MkdirAll(dir, 0755)
	if err != nil {
		return err
	}

	for i := 0; i < fileCount; i++ {
		if err := createFileIfNotExist(size, path.Join(dir, fmt.Sprintf("%s_%d", filePrefix, i))); err != nil {
			return err
		}
	}

	return nil
}

func createFileIfNotExist(size int64, name string) error {
	_, err := os.Stat(name)
	if os.IsNotExist(err) {
		fd, err := os.Create(name)
		if err != nil {
			return err
		}
		defer fd.Close()

		return fd.Truncate(size)
	}

	return err
}
