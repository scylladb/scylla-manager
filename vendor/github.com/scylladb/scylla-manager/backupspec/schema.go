// Copyright (C) 2025 ScyllaDB

package backupspec

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// AlternatorSchema defines the json format of the whole alternator schema file.
type AlternatorSchema struct {
	Tables []AlternatorTableSchema `json:"tables,omitempty"`
}

// AlternatorTableSchema defines the json format of the table alternator schema.
type AlternatorTableSchema struct {
	Describe *types.TableDescription      `json:"describe,omitempty"`
	Tags     []types.Tag                  `json:"tags,omitempty"`
	TTL      *types.TimeToLiveDescription `json:"ttl,omitempty"`
}
