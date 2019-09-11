package metricsext

import (
	"context"
	"sync"

	"github.com/cep21/gometrics/metrics"
	"github.com/cep21/gometrics/metrics/internal"
)

// MultiSink aggregations metrics from multiple sinks. It is thread safe.
type MultiSink struct {
	sinks map[metrics.AggregationSink]struct{}
	mu    sync.Mutex
}

// Aggregate sends metrics to each added sink
func (m *MultiSink) Aggregate(ctx context.Context, aggs []metrics.TimeSeriesAggregation) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var errs []error
	for k := range m.sinks {
		if err := k.Aggregate(ctx, aggs); err != nil {
			errs = append(errs, err)
		}
	}
	return internal.ConsolidateErr(errs)
}

// AddSink includes a sink for aggregation
func (m *MultiSink) AddSink(s metrics.AggregationSink) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sinks == nil {
		m.sinks = make(map[metrics.AggregationSink]struct{})
	}
	m.sinks[s] = struct{}{}
}

// RemoveSink removes a sink from aggregation
func (m *MultiSink) RemoveSink(s metrics.AggregationSink) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sinks, s)
}

var _ metrics.AggregationSink = &MultiSink{}

// MultiSource is an aggregation source that pulls metrics from multiple places.  It is thread safe
type MultiSource struct {
	sources map[metrics.AggregationSource]struct{}
	mu      sync.Mutex
}

// FlushMetrics returns metrics from all sources
func (m *MultiSource) FlushMetrics() []metrics.TimeSeriesAggregation {
	m.mu.Lock()
	defer m.mu.Unlock()
	var ret []metrics.TimeSeriesAggregation
	for s := range m.sources {
		ret = append(ret, s.FlushMetrics()...)
	}
	return ret
}

// AddSource includes a source for flushing
func (m *MultiSource) AddSource(s metrics.AggregationSource) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sources == nil {
		m.sources = make(map[metrics.AggregationSource]struct{})
	}
	m.sources[s] = struct{}{}
}

// RemoveSource removes a source, if it exists, from the flushable set
func (m *MultiSource) RemoveSource(s metrics.AggregationSource) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sources, s)
}

var _ metrics.AggregationSource = &MultiSource{}
