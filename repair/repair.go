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
	"github.com/scylladb/mermaid"
	"github.com/scylladb/mermaid/internal/dht"
	"github.com/scylladb/mermaid/internal/inexlist"
	"github.com/scylladb/mermaid/scyllaclient"
	"github.com/scylladb/mermaid/uuid"
	"go.uber.org/multierr"
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

// groupSegmentsByHost extract list of segments (token ranges) for hosts
// in a datacenter and returns a mapping from host to list of it's segments.
// If hosts is not empty the mapping contains only segments that belong to
// at least host in hosts.
func groupSegmentsByHost(dc string, hosts []string, tr TokenRangesKind, ring []*scyllaclient.TokenRange) (map[string]segments, error) {
	m := make(map[string]segments)
	hs := strset.New(hosts...)

	for _, r := range ring {
		if len(r.Hosts[dc]) == 0 {
			return nil, errors.Errorf("dc %s does not contain all the tokens please check the replication strategy", dc)
		}

		// ignore segments that not are not replicated by any of the hosts in any DC
		if !hs.IsEmpty() {
			ok := false
			for _, dcHosts := range r.Hosts {
				for _, h := range dcHosts {
					if hs.Has(h) {
						ok = true
						break
					}
				}
			}
			if !ok {
				continue
			}
		}

		// select hosts based on kind of token ranges
		var hosts []string
		switch tr {
		case PrimaryTokenRanges:
			hosts = r.Hosts[dc][0:1]
		case NonPrimaryTokenRanges:
			hosts = r.Hosts[dc][1:]
		case AllTonenRanges:
			hosts = r.Hosts[dc]
		default:
			return nil, errors.New("no token ranges specified")
		}

		for _, h := range hosts {
			if r.StartToken > r.EndToken {
				m[h] = append(m[h],
					&segment{StartToken: dht.Murmur3MinToken, EndToken: r.EndToken},
					&segment{StartToken: r.StartToken, EndToken: dht.Murmur3MaxToken},
				)
			} else {
				m[h] = append(m[h], &segment{StartToken: r.StartToken, EndToken: r.EndToken})
			}
		}
	}

	return m, nil
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
		for _, token := range p.SegmentErrorStartTokens {
			if _, ok := shards[i].containStartToken(token); !ok {
				return errors.Errorf("shard %d: no segment for (failed) start token %d", p.Shard, token)
			}
		}
	}

	return nil
}

// validateSubset checks if sub is a subset of all, if not returns error with
// diff information.
func validateSubset(sub []string, all []string) error {
	if len(sub) == 0 {
		return nil
	}

	s := strset.New(sub...)
	for _, t := range all {
		s.Remove(t)
	}
	if !s.IsEmpty() {
		return errors.New(s.String())
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

	var (
		idx   = 0
		total int
	)
	for i, u := range run.Units {
		end := sort.Search(len(prog), func(j int) bool {
			return prog[j].Unit > i
		})
		p := aggregateUnitProgress(u, prog[idx:end])
		total += p.PercentComplete
		v.Units = append(v.Units, p)
		idx = end
	}

	v.PercentComplete = total / len(run.Units)

	return v
}

func aggregateUnitProgress(u Unit, prog []*RunProgress) UnitProgress {
	v := UnitProgress{Unit: u}

	if len(prog) == 0 {
		return v
	}

	var (
		host   = prog[0].Host
		total  int
		shards []ShardProgress
	)
	for _, p := range prog {
		if p.Host != host {
			v.Nodes = append(v.Nodes, NodeProgress{
				progress: progress{PercentComplete: total / len(shards)},
				Host:     host,
				Shards:   shards,
			})
			host = p.Host
			total = 0
			shards = nil
		}

		c := p.PercentComplete()
		total += c
		shards = append(shards, ShardProgress{
			progress:       progress{PercentComplete: c},
			SegmentCount:   p.SegmentCount,
			SegmentSuccess: p.SegmentSuccess,
			SegmentError:   p.SegmentError,
		})
	}
	v.Nodes = append(v.Nodes, NodeProgress{
		progress: progress{PercentComplete: total / len(shards)},
		Host:     host,
		Shards:   shards,
	})

	total = 0
	for _, n := range v.Nodes {
		total += n.PercentComplete
	}
	v.PercentComplete = total / len(v.Nodes)

	sort.Slice(v.Nodes, func(i, j int) bool {
		return v.Nodes[i].PercentComplete > v.Nodes[j].PercentComplete
	})
	return v
}

func sortUnits(units []Unit, inclExcl inexlist.InExList) {
	positions := make(map[string]int)
	for _, u := range units {
		if p := inclExcl.FirstMatch(u.Keyspace); p >= 0 {
			positions[u.Keyspace] = p
		} else {
			positions[u.Keyspace] = inclExcl.Size()
		}
	}

	sort.SliceStable(units, func(i, j int) bool {
		h1 := xxhash.Sum64String(units[i].Keyspace)
		h2 := xxhash.Sum64String(units[j].Keyspace)
		return h1 > h2
	})

	if len(positions) == 0 {
		return
	}

	sort.SliceStable(units, func(i, j int) bool {
		return positions[units[i].Keyspace] < positions[units[j].Keyspace]
	})
}

func validateKeyspaceFilters(filters []string) error {
	var errs error
	for i, f := range filters {
		err := validateKeyspaceFilter(filters[i])
		if err != nil {
			errs = multierr.Append(errs, errors.Wrapf(err, "%q on position %d", f, i))
			continue
		}
	}
	return mermaid.ErrValidate(errs, "invalid filters")
}

func validateKeyspaceFilter(filter string) error {
	if filter == "*" || filter == "!*" {
		return nil
	}
	if strings.HasPrefix(filter, ".") {
		return errors.New("missing keyspace")
	}
	return nil
}

func decorateKeyspaceFilters(filters []string) []string {
	if len(filters) == 0 {
		filters = append(filters, "*.*")
	}

	for i, f := range filters {
		if strings.Contains(f, ".") {
			continue
		}
		if strings.HasSuffix(f, "*") {
			filters[i] = strings.TrimSuffix(f, "*") + "*.*"
		} else {
			filters[i] += ".*"
		}
	}

	filters = append(filters, "!system.*")

	return filters
}

func decorateDCFilters(filters []string) []string {
	if len(filters) == 0 {
		filters = append(filters, "*")
	}
	return filters
}
