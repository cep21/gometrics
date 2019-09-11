package metricsext

import "github.com/cep21/gometrics/metrics"

// LocklessValueAggregator observes values without any locking (it is not thread safe)
type LocklessValueAggregator struct {
	Bucketer    metrics.Bucketer
	sampleCount int32
	maximum     float64
	minimum     float64
	sum         float64
	sumSquare   float64
	firstValue  float64
	lastValue   float64
}

var _ metrics.ValueAggregator = &LocklessValueAggregator{}

// Aggregate returns an aggregation of all the observed values
func (a *LocklessValueAggregator) Aggregate() metrics.ValueAggregation {
	ret := metrics.ValueAggregation{
		SampleCount: a.sampleCount,
		Maximum:     a.maximum,
		Minimum:     a.minimum,
		Sum:         a.sum,
		SumSquare:   a.sumSquare,
		FirstValue:  a.firstValue,
		LastValue:   a.lastValue,
	}
	if a.Bucketer != nil {
		ret.Buckets = a.Bucketer.Buckets()
	}
	return ret
}

// Observe adds a value to this aggregator (and any bucketer it has)
func (a *LocklessValueAggregator) Observe(value float64) {
	if a.sampleCount == 0 {
		a.firstValue = value
	}
	a.lastValue = value
	if a.sampleCount == 0 || a.minimum > value {
		a.minimum = value
	}
	if a.sampleCount == 0 || a.maximum < value {
		a.maximum = value
	}
	a.sampleCount++
	a.sum += value
	a.sumSquare += value * value
	if a.Bucketer != nil {
		a.Bucketer.Observe(value)
	}
}
