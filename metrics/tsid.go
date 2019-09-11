package metrics

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cep21/gometrics/metrics/internal"
)

// TimeSeriesIdentifier identifies a time series
type TimeSeriesIdentifier struct {
	// Treat MetricName as read-only please.  We could make this private and use a getter, but it would be inconsistent
	// with *Dimensions*.  Dragons beware if you modify the Metric name of *any* TimeSeriesIdentifier.
	MetricName string
	// Treat Dimensions map as read-only please.  We do not enforce this in the API because Go does not have
	//   const like that.  Dragons beware if you modify the Dimensions parameter of *any* TimeSeriesIdentifier
	Dimensions map[string]string
}

// Just used for UID generation
type kvPairs struct {
	key string
	val string
}

// UID returns a string that uniquely identifies this identifier
func (t *TimeSeriesIdentifier) UID() string {
	var ret strings.Builder
	ret.Grow(250) // seems a good size.
	//ret := sb.Get()
	//defer sb.Put(ret)
	mustWrite(ret.WriteString(t.MetricName))
	if len(t.Dimensions) > 0 {
		toSort := make([]kvPairs, len(t.Dimensions))
		for k, v := range t.Dimensions {
			toSort = append(toSort, kvPairs{
				key: k,
				val: v,
			})
		}
		sort.Slice(toSort, func(i, j int) bool {
			return toSort[i].key < toSort[j].key
		})
		for _, s := range toSort {
			mustNotErr(ret.WriteByte(0))
			mustWrite(ret.WriteString(s.key))
			mustNotErr(ret.WriteByte(0))
			mustWrite(ret.WriteString(s.val))
		}
	}
	return ret.String()
}

func (t *TimeSeriesIdentifier) String() string {
	var ret strings.Builder
	mustWrite(ret.WriteString(t.MetricName))
	if len(t.Dimensions) > 0 {
		toSort := make([]kvPairs, len(t.Dimensions))
		for k, v := range t.Dimensions {
			toSort = append(toSort, kvPairs{
				key: k,
				val: v,
			})
		}
		sort.Slice(toSort, func(i, j int) bool {
			return toSort[i].key < toSort[j].key
		})
		for _, s := range toSort {
			mustWrite(fmt.Fprintf(&ret, " %s=%s", s.key, s.val))
		}
	}
	return ret.String()
}

func mustWrite(_ int, err error) {
	if err != nil {
		panic(err)
	}
}

func mustNotErr(err error) {
	if err != nil {
		panic(err)
	}
}

func uniqueCopy(t TimeSeriesIdentifier) TimeSeriesIdentifier {
	return TimeSeriesIdentifier{
		MetricName: t.MetricName,
		Dimensions: internal.CopyOfMap(t.Dimensions),
	}
}
