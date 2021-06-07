// Copyright (C) 2017 ScyllaDB

package metrics

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/scylla-manager/pkg/testutils"
)

func TestDeleteMatching(t *testing.T) {
	g := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "test",
		Name:      "test",
		Help:      "test,",
	}, []string{"cluster"})

	g.WithLabelValues("c0").Add(1)
	g.WithLabelValues("c1").Add(1)
	g.WithLabelValues("c2").Add(1)
	DeleteMatching(g, LabelMatcher("cluster", "c0"))

	text := Dump(t, g)

	testutils.SaveGoldenTextFileIfNeeded(t, text)
	golden := testutils.LoadGoldenTextFile(t)
	if diff := cmp.Diff(text, golden); diff != "" {
		t.Error(diff)
	}
}

func TestSetGaugeVecMatching(t *testing.T) {
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "test",
		Name:      "test",
		Help:      "test,",
	}, []string{"cluster"})

	g.WithLabelValues("c0").Set(1)
	g.WithLabelValues("c1").Set(1)
	g.WithLabelValues("c2").Set(1)
	setGaugeVecMatching(g, unspecifiedValue, LabelMatcher("cluster", "c0"))

	text := Dump(t, g)

	testutils.SaveGoldenTextFileIfNeeded(t, text)
	golden := testutils.LoadGoldenTextFile(t)
	if diff := cmp.Diff(text, golden); diff != "" {
		t.Error(diff)
	}
}
