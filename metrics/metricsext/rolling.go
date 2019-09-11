package metricsext

import (
	"sync"
	"time"

	"github.com/cep21/gometrics/metrics"
)

// RollingAggregation aggregates data into time rolling buckets
type RollingAggregation struct {
	// Default does not use buckets
	AggregatorFactory func() metrics.ValueAggregator

	// Default is 1 minute
	BucketSize time.Duration
	// Default is time.Now
	Now func() time.Time

	buckets         map[int64]metrics.ValueAggregator
	mu              sync.Mutex
	lastReportedIdx int64
}

func (t *RollingAggregation) bucketSize() time.Duration {
	if t.BucketSize == 0 {
		return time.Minute
	}
	return t.BucketSize
}

func (t *RollingAggregation) createValueAggregator() metrics.ValueAggregator {
	if t.AggregatorFactory == nil {
		return &LocklessValueAggregator{}
	}
	return t.AggregatorFactory()
}

func (t *RollingAggregation) now() time.Time {
	if t.Now == nil {
		return time.Now()
	}
	return t.Now()
}

// CollectMetrics returns an aggregation for data in any previous time window bucket
func (t *RollingAggregation) CollectMetrics() []metrics.TimeWindowAggregation {
	currentIdx := t.bucketIndex(t.now())
	t.mu.Lock()
	defer t.mu.Unlock()

	var ret []metrics.TimeWindowAggregation
	for idx, agg := range t.buckets {
		if idx < currentIdx {
			values := agg.Aggregate()
			ret = append(ret, metrics.TimeWindowAggregation{
				Va: values,
				Tw: metrics.TimeWindow{
					Start:    time.Unix(0, idx*t.bucketSize().Nanoseconds()),
					Duration: t.bucketSize(),
				},
			})
			delete(t.buckets, idx)
			if idx > t.lastReportedIdx {
				t.lastReportedIdx = idx
			}
		}
	}

	// If I'm at index 20, and the last reported index is 19, that's fine (we're still aggregating 20)
	// If I'm at 20, and the last reported index is 18, that means 19 was fully empty, so report an empty aggregation
	// TODO: Really need more unit tests here
	if t.lastReportedIdx < currentIdx-1 {
		ret = append(ret, metrics.TimeWindowAggregation{
			Tw: metrics.TimeWindow{
				Start:    time.Unix(0, t.lastReportedIdx*t.bucketSize().Nanoseconds()),
				Duration: t.bucketSize() * time.Duration(currentIdx-t.lastReportedIdx),
			},
			Va: t.createValueAggregator().Aggregate(),
		})
		t.lastReportedIdx = currentIdx
	}
	return ret
}

// Observe puts this value in a bucket for the current time
func (t *RollingAggregation) Observe(value float64) {
	aggIdx := t.bucketIndex(t.now())
	t.mu.Lock()
	defer t.mu.Unlock()
	currentBucket, exists := t.buckets[aggIdx]
	if !exists {
		if t.buckets == nil {
			t.buckets = make(map[int64]metrics.ValueAggregator)
		}
		currentBucket = t.createValueAggregator()
		t.buckets[aggIdx] = currentBucket
	}
	currentBucket.Observe(value)
}

func (t *RollingAggregation) bucketIndex(when time.Time) int64 {
	bucketTime := when.Truncate(t.bucketSize())
	return bucketTime.UnixNano() / t.bucketSize().Nanoseconds()
}

var _ metrics.Aggregator = &RollingAggregation{}
