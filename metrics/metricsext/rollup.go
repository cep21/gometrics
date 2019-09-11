package metricsext

import (
	"context"

	"github.com/cep21/gometrics/metrics"
	"github.com/cep21/gometrics/metrics/internal"
)

// RollupSink understands "rollup" concepts on time series and expands TimeSeriesAggregation values into their rollups
type RollupSink struct {
	Sink metrics.AggregationSink
	// Note: You probably want metris.Registry explicitly here.  You probably don't want a wrapped registry, or you
	//       may *add back* the dimensions you're trying to remove when you make the new time series.
	//       I actually thought about making this *metrics.Registry instead ...
	TSSource metrics.TimeSeriesSource
}

type metadataType int

const (
	metaDataRollups metadataType = iota
)

var _ metrics.AggregationSink = &RollupSink{}

// GetRollups returns existing rollups from time series metadata
func GetRollups(tsm metrics.TimeSeriesMetadata) [][]string {
	retI := tsm.Value(metaDataRollups)
	if retI == nil {
		return nil
	}
	if ret, ok := retI.([][]string); ok {
		return ret
	}
	return nil
}

// WithRollup creates a metadata constructor that appends rollups to a time series's metadata
func WithRollup(rollup []string) metrics.MetadataConstructor {
	// TODO: Copy rollup before we return (safer)
	return func(id metrics.TimeSeriesIdentifier, md metrics.TimeSeriesMetadata) metrics.TimeSeriesMetadata {
		existingRollups := GetRollups(md)
		newRollups := make([][]string, 0, len(existingRollups)+1)
		newRollups = append(newRollups, existingRollups...)
		newRollups = append(newRollups, rollup)
		// TODO: Uniquify the values in newRollups array
		return md.WithValue(metaDataRollups, newRollups)
	}
}

// RegistryWithRollup wraps a registry with a metric constructor that adds rollup to all time series created from it
func RegistryWithRollup(a metrics.BaseRegistry, rollup []string) metrics.BaseRegistry {
	if len(rollup) == 0 {
		return a
	}
	return WithMetadata(a, WithRollup(rollup))
}

// Aggregate does rollups of every aggregation and sends that to each source
func (r *RollupSink) Aggregate(ctx context.Context, aggs []metrics.TimeSeriesAggregation) error {
	ret := make([]metrics.TimeSeriesAggregation, 0, len(aggs))
	ret = append(ret, aggs...)
	for _, agg := range aggs {
		ret = append(ret, r.rollups(agg)...)
	}
	return r.Sink.Aggregate(ctx, ret)
}

func rollupFromTimeSeries(ts *metrics.TimeSeries, TSSource metrics.TimeSeriesSource, rollup []string) *metrics.TimeSeries {
	if len(rollup) == 0 {
		return ts
	}
	if len(ts.Tsi.Dimensions) == 0 {
		return ts
	}
	newTsDims := internal.CopyOfMap(ts.Tsi.Dimensions)
	for _, r := range rollup {
		delete(newTsDims, r)
	}
	if len(newTsDims) == len(ts.Tsi.Dimensions) {
		return ts
	}
	return TSSource.TimeSeries(metrics.TimeSeriesIdentifier{
		MetricName: ts.Tsi.MetricName,
		Dimensions: newTsDims,
	}, func(_ metrics.TimeSeriesIdentifier, md metrics.TimeSeriesMetadata) metrics.TimeSeriesMetadata {
		// It's unclear what metadata the wrapped time series should have.  Maybe just give it exactly the metadata of the parent
		// Wonder if we should merge md with ts.Tsm
		return ts.Tsm
	})
}

func (r *RollupSink) rollups(agg metrics.TimeSeriesAggregation) []metrics.TimeSeriesAggregation {
	rollups := GetRollups(agg.TS.Tsm)
	ret := make([]metrics.TimeSeriesAggregation, 0, len(rollups))
	var set tsSet
	set.Add(agg.TS)
	for _, rollup := range rollups {
		tsRollup := rollupFromTimeSeries(agg.TS, r.TSSource, rollup)
		if set.Contains(tsRollup) {
			continue
		}
		set.Add(tsRollup)
		ret = append(ret, metrics.TimeSeriesAggregation{
			TS:          tsRollup,
			Aggregation: agg.Aggregation,
		})
	}
	return ret
}

type tsSet struct {
	s map[*metrics.TimeSeries]struct{}
}

func (t *tsSet) Add(ts *metrics.TimeSeries) {
	if t.s == nil {
		t.s = make(map[*metrics.TimeSeries]struct{})
	}
	t.s[ts] = struct{}{}
}

func (t *tsSet) Contains(ts *metrics.TimeSeries) bool {
	_, exists := t.s[ts]
	return exists
}
