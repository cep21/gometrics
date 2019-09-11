package metrics

// ValueAggregation is an aggregation of distinct values.  The values themselves are timeless.
type ValueAggregation struct {
	SampleCount int32
	Maximum     float64
	Minimum     float64
	Sum         float64
	SumSquare   float64
	FirstValue  float64
	LastValue   float64
	Buckets     []Bucket
}

// Union merges and returns this aggregation with another
func (a ValueAggregation) Union(other ValueAggregation) ValueAggregation {
	return ValueAggregation{
		FirstValue:  a.FirstValue,
		LastValue:   other.LastValue,
		SampleCount: a.SampleCount + other.SampleCount,
		Maximum:     maxFloat(a.Maximum, other.Maximum),
		Minimum:     minFloat(a.Minimum, other.Minimum),
		Sum:         a.Sum + other.Sum,
		SumSquare:   a.SumSquare + other.SumSquare,
		Buckets:     bucketMerge(a.Buckets, other.Buckets),
	}
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
