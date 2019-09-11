package metricsext

import (
	"context"

	"github.com/cep21/gometrics/metrics"
)

// CompressionSink can aggregation two aggregations into a single aggregation if they match on both time series and
// window.
type CompressionSink struct {
	Sink metrics.AggregationSink
}

type compressableAgg struct {
	ts *metrics.TimeSeries
	tw metrics.TimeWindow
}

var _ metrics.AggregationSink = &CompressionSink{}

// Aggregate multiple tsa into a single tsa if they match on both time series and time window
func (r *CompressionSink) Aggregate(ctx context.Context, aggs []metrics.TimeSeriesAggregation) error {
	byTs := make(map[compressableAgg][]metrics.TimeSeriesAggregation, len(aggs))
	for _, agg := range aggs {
		key := compressableAgg{
			ts: agg.TS,
			tw: agg.Aggregation.Tw,
		}
		byTs[key] = append(byTs[key], agg)
	}
	nextLevel := make([]metrics.TimeSeriesAggregation, 0, len(aggs))
	for k, v := range byTs {
		if len(v) == 0 {
			nextLevel = append(nextLevel, v[0])
		}
		totalAgg := v[0].Aggregation
		for i := 1; i < len(v); i++ {
			totalAgg = totalAgg.Union(v[i].Aggregation)
		}
		nextLevel = append(nextLevel, metrics.TimeSeriesAggregation{
			TS:          k.ts,
			Aggregation: totalAgg,
		})
	}
	return r.Sink.Aggregate(ctx, nextLevel)
}
