package metricsext

import (
	"time"

	"github.com/cep21/gometrics/metrics"
)

// DurationObserver wraps an observer to allow reporting durations, rather than flat float64 objects
type DurationObserver struct {
	observer metrics.Observer
}

// Observe reports to the wrapped observer the duration as a Second time value
func (t *DurationObserver) Observe(d time.Duration) {
	t.observer.Observe(d.Seconds())
}

// Duration is similar to Float, but attaches metadata of the unit "seconds" to time series
func Duration(a metrics.BaseRegistry, metricName string, dimensions map[string]string) *DurationObserver {
	ts := a.TimeSeries(metrics.TimeSeriesIdentifier{
		MetricName: metricName,
		Dimensions: dimensions,
	}, func(_ metrics.TimeSeriesIdentifier, tsmd metrics.TimeSeriesMetadata) metrics.TimeSeriesMetadata {
		return tsmd.WithValue(metrics.MetaDataUnit, "Seconds")
	})
	obs := a.Observer(ts)
	return &DurationObserver{
		observer: obs,
	}
}

// Counter returns an observer set with the metric type counter
func Counter(a metrics.BaseRegistry, metricName string, dimensions map[string]string) metrics.Observer {
	ts := a.TimeSeries(metrics.TimeSeriesIdentifier{
		MetricName: metricName,
		Dimensions: dimensions,
	}, func(_ metrics.TimeSeriesIdentifier, tsmd metrics.TimeSeriesMetadata) metrics.TimeSeriesMetadata {
		return tsmd.WithValue(metrics.MetaDataTimeSeriesType, metrics.TSTypeCounter)
	})
	return a.Observer(ts)
}

// Gauge returns an observer set with the metric type Gauge
func Gauge(a metrics.BaseRegistry, metricName string, dimensions map[string]string) metrics.Observer {
	ts := a.TimeSeries(metrics.TimeSeriesIdentifier{
		MetricName: metricName,
		Dimensions: dimensions,
	}, func(_ metrics.TimeSeriesIdentifier, tsmd metrics.TimeSeriesMetadata) metrics.TimeSeriesMetadata {
		return tsmd.WithValue(metrics.MetaDataTimeSeriesType, metrics.TSTypeGauge)
	})
	return a.Observer(ts)
}

// Float simply returns an observer for a time series with no special metadata
func Float(a metrics.BaseRegistry, metricName string, dimensions map[string]string) metrics.Observer {
	ts := a.TimeSeries(metrics.TimeSeriesIdentifier{
		MetricName: metricName,
		Dimensions: dimensions,
	}, nil)
	return a.Observer(ts)
}

// WithDimensions wraps a registry with a registry that adds default dimensions to created time series
func WithDimensions(a metrics.BaseRegistry, dimensions map[string]string) metrics.BaseRegistry {
	if asW, ok := a.(*wrappedRegistry); ok {
		return &wrappedRegistry{
			BaseRegistry: asW.BaseRegistry,
			dimensions:   mergeMapsFast(dimensions, asW.dimensions),
			metadata:     asW.metadata,
		}
	}
	return &wrappedRegistry{
		BaseRegistry: a,
		dimensions:   dimensions,
	}
}

// WithMetadata wraps a registry with a metadata constructor for all time series
func WithMetadata(a metrics.BaseRegistry, metadata metrics.MetadataConstructor) metrics.BaseRegistry {
	if metadata == nil {
		return a
	}
	if asW, ok := a.(*wrappedRegistry); ok {
		return &wrappedRegistry{
			BaseRegistry: asW.BaseRegistry,
			dimensions:   asW.dimensions,
			metadata: func(tsi metrics.TimeSeriesIdentifier, mtd metrics.TimeSeriesMetadata) metrics.TimeSeriesMetadata {
				if asW.metadata == nil {
					return metadata(tsi, mtd)
				}
				return asW.metadata(tsi, metadata(tsi, mtd))
			},
		}
	}
	return &wrappedRegistry{
		BaseRegistry: a,
		metadata:     metadata,
	}
}

type wrappedRegistry struct {
	metrics.BaseRegistry
	dimensions map[string]string
	metadata   metrics.MetadataConstructor
}

// TimeSeries returns the unique time series for an identifier
func (u *wrappedRegistry) TimeSeries(tsi metrics.TimeSeriesIdentifier, metadata metrics.MetadataConstructor) *metrics.TimeSeries {
	tsi.Dimensions = mergeMapsFast(u.dimensions, tsi.Dimensions)
	return u.BaseRegistry.TimeSeries(tsi, func(tsi metrics.TimeSeriesIdentifier, mtd metrics.TimeSeriesMetadata) metrics.TimeSeriesMetadata {
		if metadata == nil && u.metadata == nil {
			return mtd
		}
		if u.metadata == nil {
			return metadata(tsi, mtd)
		}
		if metadata == nil {
			return u.metadata(tsi, mtd)
		}
		return u.metadata(tsi, metadata(tsi, mtd))
	})
}

// SingleValue helps create a TimeWindowAggregation of a single value at the current timestamp
func SingleValue(value float64) metrics.TimeWindowAggregation {
	va := LocklessValueAggregator{}
	va.Observe(value)
	return metrics.TimeWindowAggregation{
		Va: va.Aggregate(),
		Tw: metrics.TimeWindow{
			Start: time.Now(),
		},
	}
}

type onDemandFlush struct {
	o metrics.OnDemandFlushable
	r metrics.TimeSeriesSource
}

func (o onDemandFlush) FlushMetrics() []metrics.TimeSeriesAggregation {
	return o.o.CurrentMetrics(o.r)
}

// CustomAggregation creates an aggregation source from an object that can collect metrics on demand
func CustomAggregation(r metrics.TimeSeriesSource, o metrics.OnDemandFlushable) metrics.AggregationSource {
	return onDemandFlush{
		o: o,
		r: r,
	}
}

func mergeMapsCopy(m1 map[string]string, m2 map[string]string) map[string]string {
	ret := make(map[string]string, len(m1)+len(m2))
	for k, v := range m1 {
		ret[k] = v
	}
	for k, v := range m2 {
		ret[k] = v
	}
	return ret
}

func mergeMapsFast(m1 map[string]string, m2 map[string]string) map[string]string {
	if len(m1) == 0 {
		return m2
	}
	if len(m2) == 0 {
		return m1
	}
	return mergeMapsCopy(m1, m2)
}
