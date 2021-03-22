// Copyright (C) 2017 ScyllaDB

package downloader

type TableDirModeValue TableDirMode

func (v *TableDirModeValue) String() string {
	return TableDirMode(*v).String()
}

func (v *TableDirModeValue) Set(s string) error {
	return (*TableDirMode)(v).UnmarshalText([]byte(s))
}

func (v *TableDirModeValue) Type() string {
	return "string"
}

func (v *TableDirModeValue) Value() TableDirMode {
	return TableDirMode(*v)
}
