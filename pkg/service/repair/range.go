// Copyright (C) 2017 ScyllaDB

package repair

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/cespare/xxhash/v2"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/scylla-manager/pkg/dht"
	"github.com/scylladb/scylla-manager/pkg/scyllaclient"
)

type tableTokenRange struct {
	Keyspace        string
	Table           string
	Pos             int
	StartToken      int64
	EndToken        int64
	Replicas        []string
	FullyReplicated bool
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

// fixRanges ensures that start token < end token at all times.
func fixRanges(ranges []*tableTokenRange) []*tableTokenRange {
	needsSplitting := 0
	for _, tr := range ranges {
		if tr.StartToken > tr.EndToken {
			needsSplitting++
		}
	}
	if needsSplitting == 0 {
		return ranges
	}

	v := make([]*tableTokenRange, 0, len(ranges)+needsSplitting)
	for _, tr := range ranges {
		if tr.StartToken > tr.EndToken {
			x := new(tableTokenRange)
			y := new(tableTokenRange)

			*x, *y = *tr, *tr
			x.StartToken = dht.Murmur3MinToken
			y.EndToken = dht.Murmur3MaxToken

			v = append(v, x, y)
		} else {
			v = append(v, tr)
		}
	}
	return v
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
	target      Target
	hostDC      map[string]string
	dcs         *strset.Set
	ignoreHosts *strset.Set

	prototypes []*tableTokenRange
	pos        int
}

func newTableTokenRangeBuilder(target Target, hostDC map[string]string) *tableTokenRangeBuilder {
	r := &tableTokenRangeBuilder{
		target:      target,
		hostDC:      hostDC,
		dcs:         strset.New(target.DC...),
		ignoreHosts: strset.New(target.IgnoreHosts...),
	}

	return r
}

func (b *tableTokenRangeBuilder) Add(ranges []scyllaclient.TokenRange) *tableTokenRangeBuilder {
	for _, tr := range ranges {
		if b.shouldAdd(tr) {
			b.add(tr)
		}
	}
	return b
}

func (b *tableTokenRangeBuilder) shouldAdd(tr scyllaclient.TokenRange) bool {
	if b.target.Host != "" && !b.isReplica(b.target.Host, tr.Replicas) {
		return false
	}

	if len(b.filteredReplicas(tr.Replicas)) == 0 {
		return false
	}

	return true
}

func (b *tableTokenRangeBuilder) isReplica(host string, replicas []string) bool {
	return strset.New(replicas...).Has(host)
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
		if b.ignoreHosts.Has(r) {
			continue
		}
		if b.dcs.Has(b.hostDC[r]) {
			out = append(out, r)
		}
	}

	return
}

// MaxParallelRepairs returns the maximal number of parallel repairs calculated
// as max_parallel = floor(# of nodes / keyspace RF).
func (b *tableTokenRangeBuilder) MaxParallelRepairs() int {
	if len(b.prototypes) == 0 {
		return 0
	}

	allNodes := strset.New()
	for _, tr := range b.prototypes {
		for _, node := range tr.Replicas {
			allNodes.Add(node)
		}
	}
	rf := len(b.prototypes[0].Replicas)

	return allNodes.Size() / rf
}

// FullyReplicated returns whether or not the keyspace is fully replicated
// True if # of nodes == keyspace RF.
func (b *tableTokenRangeBuilder) FullyReplicated() bool {
	if len(b.prototypes) == 0 {
		return true
	}

	allNodes := strset.New()
	for _, tr := range b.prototypes {
		for _, node := range tr.Replicas {
			allNodes.Add(node)
		}
	}
	rf := len(b.prototypes[0].Replicas)

	return allNodes.Size() == rf
}

func (b *tableTokenRangeBuilder) Build(unit Unit) (out []*tableTokenRange) {
	fullyReplicated := b.FullyReplicated()

	for _, table := range unit.Tables {
		for _, p := range b.prototypes {
			ttr := *p
			ttr.Keyspace = unit.Keyspace
			ttr.Table = table
			ttr.FullyReplicated = fullyReplicated
			out = append(out, &ttr)
		}
	}
	return
}
