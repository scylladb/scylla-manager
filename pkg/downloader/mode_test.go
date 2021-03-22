// Copyright (C) 2017 ScyllaDB

package downloader

import "testing"

func TestTableDirModeMarshalUnmarshalText(t *testing.T) {
	t.Parallel()

	for _, k := range []TableDirMode{DefaultTableDirMode, UploadTableDirMode, SSTableLoaderTableDirMode} {
		b, err := k.MarshalText()
		if err != nil {
			t.Error(k, err)
		}
		var v TableDirMode
		if err := v.UnmarshalText(b); err != nil {
			t.Error(err)
		}
		if k != v {
			t.Errorf("Got %s, expected %s", v, k)
		}
	}
}
