package metrics

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// GoMetricsEncoder encodes OTEL metrics in a flat key-value format
// similar to go-metrics' WriteOnce output.
// Reference: https://github.com/rcrowley/go-metrics/blob/master/writer.go#L20
type GoMetricsEncoder struct{}

func (e *GoMetricsEncoder) Encode(v any) error {
	rm, ok := v.(*metricdata.ResourceMetrics)
	if !ok {
		return json.NewEncoder(os.Stdout).Encode(v)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Histogram[int64]:
				hs := HistogramStats[int64]{}
				var prefix string
				for _, dp := range data.DataPoints {
					hs.AddDataPoint(dp)
					prefix = metricPrefix(m.Name, dp.Attributes)
				}
				hs.Stat()
				hs.WriteOnce(prefix, os.Stdout)
			case metricdata.Histogram[float64]:
				hs := HistogramStats[float64]{}
				var prefix string
				for _, dp := range data.DataPoints {
					hs.AddDataPoint(dp)
					prefix = metricPrefix(m.Name, dp.Attributes)
				}
				hs.Stat()
				hs.WriteOnce(prefix, os.Stdout)
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					prefix := metricPrefix(m.Name, dp.Attributes)
					fmt.Fprintf(os.Stdout, "sum %s\n", prefix)
					fmt.Fprintf(os.Stdout, "  value:       %9d\n", dp.Value)
				}
			case metricdata.Sum[float64]:
				for _, dp := range data.DataPoints {
					prefix := metricPrefix(m.Name, dp.Attributes)
					fmt.Fprintf(os.Stdout, "sum %s\n", prefix)
					fmt.Fprintf(os.Stdout, "  value:       %f\n", dp.Value)
				}
			case metricdata.Gauge[int64]:
				for _, dp := range data.DataPoints {
					prefix := metricPrefix(m.Name, dp.Attributes)
					fmt.Fprintf(os.Stdout, "gauge %s\n", prefix)
					fmt.Fprintf(os.Stdout, "  value:       %9d\n", dp.Value)
				}
			case metricdata.Gauge[float64]:
				for _, dp := range data.DataPoints {
					prefix := metricPrefix(m.Name, dp.Attributes)
					fmt.Fprintf(os.Stdout, "gauge %s\n", prefix)
					fmt.Fprintf(os.Stdout, "  value:       %f\n", dp.Value)
				}
			}
		}
	}
	return nil
}

func metricPrefix(name string, attrs attribute.Set) string {
	if attrs.Len() == 0 {
		return name
	}
	var parts []string
	iter := attrs.Iter()
	for iter.Next() {
		kv := iter.Attribute()
		parts = append(parts, fmt.Sprintf("%s=%s", string(kv.Key), kv.Value.Emit()))
	}
	return fmt.Sprintf("%s[%s]", name, strings.Join(parts, ","))
}
