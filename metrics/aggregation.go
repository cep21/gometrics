package metrics

import (
	"context"
)

// TimeWindowAggregation is an aggregation of values inside a time window
type TimeWindowAggregation struct {
	Va ValueAggregation
	Tw TimeWindow
}

// Union Merges two aggregations inside a time period
func (a TimeWindowAggregation) Union(other TimeWindowAggregation) TimeWindowAggregation {
	return TimeWindowAggregation{
		Va: a.Va.Union(other.Va),
		Tw: a.Tw.Union(other.Tw),
	}
}

// Observer can record a value. The terminology is borrowed from https://godoc.org/golang.org/x/net/internal/timeseries#Observable
// And https://github.com/prometheus/client_golang
type Observer interface {
	Observe(value float64)
}

// Aggregator is any object that can both record values as well as report aggregations of those values
type Aggregator interface {
	MetricCollector
	Observer
}

type nopAggregator struct {
}

func (n *nopAggregator) CollectMetrics() []TimeWindowAggregation {
	return nil
}

func (n *nopAggregator) Observe(value float64) {
}

var _ Aggregator = &nopAggregator{}

// MetadataConstructor wraps default meta data of a time series with the time series new meta data
type MetadataConstructor func(TimeSeriesIdentifier, TimeSeriesMetadata) TimeSeriesMetadata

// AggregationConstructor creates the correct aggregator for a unique time series.
type AggregationConstructor func(ts *TimeSeries) Aggregator

// TimeSeriesSource can create unique time series.
type TimeSeriesSource interface {
	// TimeSeries returns a unique time series for an identifier.  The first time this is called, use MetadataConstructor
	// to create the time series meta data.  After that, the metadata constructor should be ignored.
	TimeSeries(tsi TimeSeriesIdentifier, metadata MetadataConstructor) *TimeSeries
}

// BaseRegistry is the core interface users of the metrics platform should pass around.  This simple interface should be
// enough to use the entire metrics platform.
type BaseRegistry interface {
	// Get a unique time series for some meta data
	TimeSeriesSource
	// Observer allows creating default observers for a time series
	Observer(ts *TimeSeries) Observer
	// Set an explicit metric collector for a time series, if one does not exist.  Then, return the current collector.
	GetOrSet(ts *TimeSeries, mc MetricCollectorConstructor) MetricCollector
}

// OnDemandFlushable is implemented by objects that can flush themselves on demand, don't know their time series objects
// until the flush starts, and could flush multiple time series values.
type OnDemandFlushable interface {
	CurrentMetrics(r TimeSeriesSource) []TimeSeriesAggregation
}

// ValueAggregator is anything that can observe values and return aggregations
type ValueAggregator interface {
	Observer
	// Aggregate returns the current aggregation of this observer
	Aggregate() ValueAggregation
}

// AggregationSink can receive aggregations
type AggregationSink interface {
	Aggregate(context.Context, []TimeSeriesAggregation) error
}

// AggregationSource produces aggregations for many time series
type AggregationSource interface {
	FlushMetrics() []TimeSeriesAggregation
}

// MetricCollector produces aggregations and has no concept of being tied to a time series.
type MetricCollector interface {
	CollectMetrics() []TimeWindowAggregation
}

// MetricCollectorConstructor can create unique metric collectors for a time series
type MetricCollectorConstructor func(ts *TimeSeries) MetricCollector

// TimeSeriesAggregation ties both a unique time series to an aggregation for that time series inside a time window.
type TimeSeriesAggregation struct {
	TS          *TimeSeries
	Aggregation TimeWindowAggregation
}
