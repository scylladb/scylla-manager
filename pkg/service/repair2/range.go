// Copyright (C) 2017 ScyllaDB

package repair

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/cespare/xxhash/v2"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/mermaid/pkg/dht"
	"github.com/scylladb/mermaid/pkg/scyllaclient"
)

type tableTokenRange struct {
	Keyspace   string
	Table      string
	Pos        int
	StartToken int64
	EndToken   int64
	Replicas   []string
}

func (ttr *tableTokenRange) String() string {
	return fmt.Sprintf("keyspace=%s table=%s pos=%d starttoken=%d endtoken=%d replicas=%v",
		ttr.Keyspace, ttr.Table, ttr.Pos, ttr.StartToken, ttr.EndToken, ttr.Replicas)
}

func (ttr *tableTokenRange) ReplicaHash() uint64 {
	return replicaHash(ttr.Replicas)
}

func replicaHash(replicas []string) uint64 {
	s := make([]string, len(replicas))
	copy(s, replicas)

	sort.Strings(s)

	hash := xxhash.New()
	for i := range s {
		hash.WriteString(s[i]) // nolint: errcheck
		hash.WriteString(",")  // nolint: errcheck
	}
	return hash.Sum64()
}

// dumpRanges writes slice of tableTokenRange as a comma-separated list of pairs.
func dumpRanges(ranges []*tableTokenRange) string {
	var buf bytes.Buffer

	for i, ttr := range ranges {
		if i > 0 {
			buf.WriteByte(',')
		}

		if ttr.StartToken > ttr.EndToken {
			fmt.Fprintf(&buf, "%d:%d,%d:%d", dht.Murmur3MinToken, ttr.EndToken, ttr.StartToken, dht.Murmur3MaxToken)
		} else {
			fmt.Fprintf(&buf, "%d:%d", ttr.StartToken, ttr.EndToken)
		}
	}

	return buf.String()
}

// tableTokenRangeBuilder filters out not token ranges and replicas based on
// target.
type tableTokenRangeBuilder struct {
	target Target
	hostDC map[string]string
	dcs    *strset.Set

	prototypes []*tableTokenRange
	pos        int
}

func newTableTokenRangeBuilder(target Target, hostDC map[string]string) *tableTokenRangeBuilder {
	r := &tableTokenRangeBuilder{
		target: target,
		hostDC: hostDC,
		dcs:    strset.New(target.DC...),
	}

	return r
}

func (b *tableTokenRangeBuilder) Add(ranges []scyllaclient.TokenRange) *tableTokenRangeBuilder {
	for _, tr := range ranges {
		b.add(tr)
	}
	return b
}

func (b *tableTokenRangeBuilder) add(tr scyllaclient.TokenRange) {
	ttr := tableTokenRange{
		Pos:        b.pos,
		StartToken: tr.StartToken,
		EndToken:   tr.EndToken,
		Replicas:   b.filteredReplicas(tr.Replicas),
	}
	b.prototypes = append(b.prototypes, &ttr)
	b.pos++
}

func (b *tableTokenRangeBuilder) filteredReplicas(replicas []string) (out []string) {
	for _, r := range replicas {
		if b.dcs.Has(b.hostDC[r]) {
			out = append(out, r)
		}
	}

	return
}

func (b *tableTokenRangeBuilder) Build(unit Unit) (out []*tableTokenRange) {
	for _, table := range unit.Tables {
		for _, p := range b.prototypes {
			var ttr = *p
			ttr.Keyspace = unit.Keyspace
			ttr.Table = table
			out = append(out, &ttr)
		}
	}
	return
}
