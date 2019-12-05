// Copyright (C) 2017 ScyllaDB

package repair

import (
	"encoding/binary"
	"math"
	"sort"
	"strings"

	"github.com/cespare/xxhash"
	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/mermaid/pkg/dht"
	"github.com/scylladb/mermaid/pkg/scyllaclient"
	"github.com/scylladb/mermaid/pkg/util/uuid"
)

// validateHostsBelongToCluster checks that the hosts belong to the cluster.
func validateHostsBelongToCluster(dcMap map[string][]string, hosts ...string) error {
	if len(hosts) == 0 {
		return nil
	}

	all := strset.New()
	for _, dcHosts := range dcMap {
		for _, h := range dcHosts {
			all.Add(h)
		}
	}

	var missing []string
	for _, h := range hosts {
		if !all.Has(h) {
			missing = append(missing, h)
		}
	}
	if len(missing) > 0 {
		return errors.Errorf("no such hosts %s", strings.Join(missing, ", "))
	}
	return nil
}

// groupSegmentsByHost extracts a list of segments (token ranges) for hosts
// in a datacenter ds and returns a mapping from host to list of its segments.
// If host is not empty the mapping contains only token ranges for that host.
// If withHosts is not empty the mapping contains only token ranges that are
// replicated by at least one of the hosts.
func groupSegmentsByHost(dc string, host string, withHosts []string, tr TokenRangesKind, ring scyllaclient.Ring) map[string]segments {
	var (
		hostSegments  = make(map[string]segments)
		replicaFilter = strset.New(withHosts...)
	)

	for _, t := range ring.Tokens {
		// ignore segments that are not replicated by any of the withHosts
		if !replicaFilter.IsEmpty() {
			ok := false
			for _, h := range t.Replicas {
				if replicaFilter.Has(h) {
					ok = true
					break
				}
			}
			if !ok {
				continue
			}
		}

		// Select replicas based on token kind
		hosts := strset.New()
		switch tr {
		case DCPrimaryTokenRanges:
			for _, h := range t.Replicas {
				if ring.HostDC[h] == dc {
					hosts.Add(h)
					break
				}
			}
		case PrimaryTokenRanges:
			hosts.Add(t.Replicas[0])
		case NonPrimaryTokenRanges:
			hosts.Add(t.Replicas[1:]...)
		case AllTokenRanges:
			hosts.Add(t.Replicas...)
		default:
			panic("no token ranges specified") // this should never happen...
		}

		// Filter replicas by host (if needed)
		if host != "" {
			if hosts.Has(host) {
				hosts = strset.New(host)
			} else {
				continue
			}
		}

		// Filter replicas by dc (if needed)
		if dc != "" {
			hosts.Each(func(item string) bool {
				if ring.HostDC[item] != dc {
					hosts.Remove(item)
				}
				return true
			})
		}

		// Create and add segments for every host
		t := t
		hosts.Each(func(h string) bool {
			if t.StartToken > t.EndToken {
				hostSegments[h] = append(hostSegments[h],
					&segment{StartToken: dht.Murmur3MinToken, EndToken: t.EndToken},
					&segment{StartToken: t.StartToken, EndToken: dht.Murmur3MaxToken},
				)
			} else {
				hostSegments[h] = append(hostSegments[h], &segment{StartToken: t.StartToken, EndToken: t.EndToken})
			}
			return true
		})
	}

	// Remove segments for withHosts as they should not be coordinator hosts
	if !replicaFilter.IsEmpty() {
		replicaFilter.Each(func(h string) bool {
			delete(hostSegments, h)
			return true
		})
	}

	return hostSegments
}

// validateShardProgress checks if run progress, possibly copied from a
// different run matches the shards.
func validateShardProgress(shards []segments, prog []*RunProgress) error {
	if len(prog) != len(shards) {
		return errors.New("length mismatch")
	}

	for i, p := range prog {
		if p.Shard != i {
			return errors.Errorf("shard %d: progress for shard %d", i, p.Shard)
		}
		if p.SegmentCount != len(shards[i]) {
			return errors.Errorf("shard %d: segment count mismatch got %d expected %d", p.Shard, p.SegmentCount, len(shards[i]))
		}
		if p.LastStartToken != 0 {
			if _, ok := shards[i].containStartToken(p.LastStartToken); !ok {
				return errors.Errorf("shard %d: no segment for start token %d", p.Shard, p.LastStartToken)
			}
		}
		if len(p.SegmentErrorStartTokens) > 0 {
			return errors.Errorf("shard %d: cannot resume failed repairs dating before 2.0", p.Shard)
		}
	}

	return nil
}

// topologyHash returns hash of all the tokens.
func topologyHash(tokens []int64) uuid.UUID {
	var (
		xx = xxhash.New()
		b  = make([]byte, 8)
		u  uint64
	)
	for _, t := range tokens {
		if t >= 0 {
			u = uint64(t)
		} else {
			u = uint64(math.MaxInt64 + t)
		}
		binary.LittleEndian.PutUint64(b, u)
		xx.Write(b) // nolint
	}
	h := xx.Sum64()

	return uuid.NewFromUint64(h>>32, uint64(uint32(h)))
}

func aggregateProgress(run *Run, prog []*RunProgress) Progress {
	if len(run.Units) == 0 {
		return Progress{}
	}

	v := Progress{
		DC:          run.DC,
		TokenRanges: run.TokenRanges,
	}

	idx := 0
	for i, u := range run.Units {
		end := sort.Search(len(prog), func(j int) bool {
			return prog[j].Unit > i // nolint: scopelint
		})
		p := aggregateUnitProgress(u, prog[idx:end])
		v.progress.addProgress(p.progress)
		v.Units = append(v.Units, p)
		idx = end
	}

	v.progress.calculateProgress()
	return v
}

func aggregateUnitProgress(u Unit, prog []*RunProgress) UnitProgress {
	v := UnitProgress{Unit: u}

	if len(prog) == 0 {
		return v
	}

	var (
		host   = prog[0].Host
		nprog  progress
		shards []ShardProgress
	)
	for _, p := range prog {
		if p.Host != host {
			v.Nodes = append(v.Nodes, NodeProgress{
				progress: nprog.calculateProgress(),
				Host:     host,
				Shards:   shards,
			})
			host = p.Host
			nprog = progress{}
			shards = nil
		}

		sprog := progress{
			segmentCount:   p.SegmentCount,
			segmentSuccess: p.SegmentSuccess,
			segmentError:   p.SegmentError,
		}
		nprog.addProgress(sprog)
		v.progress.addProgress(sprog)

		shards = append(shards, ShardProgress{
			progress:       sprog.calculateProgress(),
			SegmentCount:   p.SegmentCount,
			SegmentSuccess: p.SegmentSuccess,
			SegmentError:   p.SegmentError,
		})
	}
	v.Nodes = append(v.Nodes, NodeProgress{
		progress: nprog.calculateProgress(),
		Host:     host,
		Shards:   shards,
	})

	sort.Slice(v.Nodes, func(i, j int) bool {
		return v.Nodes[i].PercentComplete > v.Nodes[j].PercentComplete
	})
	v.progress.calculateProgress()
	return v
}
