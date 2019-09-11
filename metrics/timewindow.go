package metrics

import "time"

// TimeWindow represents a period of time: duration is exclusive.
type TimeWindow struct {
	Start    time.Time
	Duration time.Duration
}

// End is the last end of this window of time (exclusive)
func (t TimeWindow) End() time.Time {
	return t.Start.Add(t.Duration)
}

// Union returns the smallest time window that includes both this window and w
func (t TimeWindow) Union(w TimeWindow) TimeWindow {
	startTime := minTime(t.Start, w.Start)
	return TimeWindow{
		Start:    startTime,
		Duration: maxTime(t.End(), w.End()).Sub(startTime),
	}
}

// Middle returns the timestamp in the middle of this window
func (t TimeWindow) Middle() time.Time {
	return t.Start.Add(t.Duration / 2)
}
