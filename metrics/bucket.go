package metrics

import "time"

// Bucket is an aggregated count of values inside a range
type Bucket struct {
	Count int32
	Start float64
	End   float64
}

// Middle is the mean of Start and End
func (b *Bucket) Middle() float64 {
	return (b.Start + b.End) / 2
}

// Bucketer is anything that can observe values and return their bucket locations
type Bucketer interface {
	Buckets() []Bucket
	Observe(float64)
}

func bucketMerge(a []Bucket, b []Bucket) []Bucket {
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}

	retBuckets := make(map[bucketRange]int32, len(a)+len(b))
	appendToRange(a, retBuckets)
	appendToRange(b, retBuckets)

	ret := make([]Bucket, 0, len(retBuckets))
	for k, v := range retBuckets {
		ret = append(ret, Bucket{
			Count: v,
			Start: k.start,
			End:   k.end,
		})
	}
	return ret
}

type bucketRange struct {
	start float64
	end   float64
}

func appendToRange(a []Bucket, ret map[bucketRange]int32) {
	for _, buck := range a {
		br := bucketRange{
			start: buck.Start,
			end:   buck.End,
		}
		ret[br] += buck.Count
	}
}

func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
