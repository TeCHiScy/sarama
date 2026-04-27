package metrics

import (
	"fmt"
	"io"
	"math"
	"slices"
	"sort"
	"time"

	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func Rate[N int64 | float64](dps []metricdata.DataPoint[N]) float64 {
	var minTime, maxTime time.Time
	var sum N
	for _, dp := range dps {
		sum += dp.Value
		if minTime.IsZero() || dp.StartTime.Before(minTime) {
			minTime = dp.StartTime
		}
		if maxTime.IsZero() || dp.Time.After(maxTime) {
			maxTime = dp.Time
		}
	}
	if minTime != maxTime {
		return float64(sum) / maxTime.Sub(minTime).Seconds()
	}
	return 0
}

type HistogramStats[N int64 | float64] struct {
	Sum    N
	Count  uint64
	Min    *N
	Max    *N
	Mean   float64
	Stddev float64
	P50    float64
	P75    float64
	P95    float64
	P99    float64
	P999   float64
	Values []float64
}

func (hs *HistogramStats[N]) AddDataPoint(dp metricdata.HistogramDataPoint[N]) {
	if dp.Count == 0 {
		return
	}

	hs.Sum += dp.Sum
	hs.Count += dp.Count
	hs.Mean = float64(hs.Sum) / float64(hs.Count)

	if v, valid := dp.Min.Value(); valid {
		if hs.Min == nil || v < *hs.Min {
			hs.Min = new(N)
			*hs.Min = v
		}
	}

	if v, valid := dp.Max.Value(); valid {
		if hs.Max == nil || v > *hs.Max {
			hs.Max = new(N)
			*hs.Max = v
		}
	}

	if len(dp.Bounds) == 0 || len(dp.BucketCounts) == 0 {
		return
	}

	var lower float64
	for i, c := range dp.BucketCounts {
		var upper float64
		if i < len(dp.Bounds) {
			upper = dp.Bounds[i]
		} else {
			upper = lower // last bucket, just increment
		}
		for range c {
			hs.Values = slices.Concat(hs.Values, slices.Repeat([]float64{(lower + upper) / 2}, int(c)))
		}
		lower = upper
	}
}

func (hs *HistogramStats[N]) Stat() {
	if len(hs.Values) == 0 {
		return
	}

	hs.Stddev = 0
	for _, v := range hs.Values {
		hs.Stddev += (v - hs.Mean) * (v - hs.Mean)
	}
	hs.Stddev = math.Sqrt(hs.Stddev / float64(len(hs.Values)))

	sort.Float64s(hs.Values)
	hs.P50 = hs.percentile(50)
	hs.P75 = hs.percentile(75)
	hs.P95 = hs.percentile(95)
	hs.P99 = hs.percentile(99)
	hs.P999 = hs.percentile(99.9)
}

func (hs *HistogramStats[N]) WriteOnce(prefix string, w io.Writer) {
	fmt.Fprintf(w, "histogram %s\n", prefix)
	fmt.Fprintf(w, "  count:       %9d\n", hs.Count)
	if hs.Min != nil {
		fmt.Fprintf(w, "  min:         %12.2f\n", float64(*hs.Min))
	}
	if hs.Max != nil {
		fmt.Fprintf(w, "  max:         %12.2f\n", float64(*hs.Max))
	}
	fmt.Fprintf(w, "  mean:        %12.2f\n", hs.Mean)
	fmt.Fprintf(w, "  stddev:      %12.2f\n", hs.Stddev)
	fmt.Fprintf(w, "  median:      %12.2f\n", hs.P50)
	fmt.Fprintf(w, "  75%%:         %12.2f\n", hs.P75)
	fmt.Fprintf(w, "  95%%:         %12.2f\n", hs.P95)
	fmt.Fprintf(w, "  99%%:         %12.2f\n", hs.P99)
	fmt.Fprintf(w, "  99.9%%:       %12.2f\n", hs.P999)
}

func (hs *HistogramStats[N]) percentile(p float64) float64 {
	if len(hs.Values) == 0 {
		return 0
	}
	if p <= 0 {
		return hs.Values[0]
	}
	if p >= 100 {
		return hs.Values[len(hs.Values)-1]
	}
	rank := p / 100 * float64(len(hs.Values)-1)
	lower := int(rank)
	upper := lower + 1
	if upper >= len(hs.Values) {
		return hs.Values[lower]
	}
	weight := rank - float64(lower)
	return hs.Values[lower]*(1-weight) + hs.Values[upper]*weight
}
