package metrics

// TimeSeriesType signals an aggregation should be treated as a special type of time series
type TimeSeriesType int

const (
	_ TimeSeriesType = iota
	// TSTypeCounter is the counter type
	TSTypeCounter
	// TSTypeGauge is the gauge type
	TSTypeGauge
)

type commonMetadataTypes int

// Note: We only explicitly define the most important metadata types and those called out in http://metrics20.org/spec/
//       (among other specs).  Users are free to associate their own metadata to a time series.
const (
	// MetaDataUnit is a unit of a time series (like Seconds)
	MetaDataUnit commonMetadataTypes = iota
	// MetaDataTimeSeriesType is the type of the time series (see TimeSeriesType)
	MetaDataTimeSeriesType
)

// TimeSeriesMetadata is extra information about a time series.  Changes to this does not change the identity of
// a time series
type TimeSeriesMetadata map[interface{}]interface{}

// Value returns the key inside this struct.  Note: This API matches context.Context
func (t TimeSeriesMetadata) Value(key interface{}) interface{} {
	return t[key]
}

// WithValue returns a copy of this struct, with the extra key/value set.  Note: this API matches context.Context
func (t TimeSeriesMetadata) WithValue(key interface{}, value interface{}) TimeSeriesMetadata {
	newMap := make(map[interface{}]interface{}, len(t)+1)
	for k, v := range t {
		newMap[k] = v
	}
	newMap[key] = value
	return newMap
}

// TimeSeries is a tracked time moving aggregation of values
type TimeSeries struct {
	Tsi TimeSeriesIdentifier
	Tsm TimeSeriesMetadata
}
