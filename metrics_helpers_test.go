package sarama

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// newTestMeterAndReader creates an OTel MeterProvider backed by a ManualReader
// for use in tests to capture and validate recorded metrics.
func newTestMeterAndReader() (metric.Meter, *sdkmetric.ManualReader) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	return provider.Meter(""), reader
}

// Common type and functions for metric validation
type metricValidator struct {
	name      string
	validator func(*testing.T, metricdata.Metrics)
}

type metricValidators []*metricValidator

func newMetricValidators() metricValidators {
	return make([]*metricValidator, 0, 32)
}

func (m *metricValidators) register(validator *metricValidator) {
	*m = append(*m, validator)
}

func (m metricValidators) run(t *testing.T, reader *sdkmetric.ManualReader) {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("failed to collect metrics: %v", err)
	}
	index := make(map[string]metricdata.Metrics)
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			index[md.Name] = md
		}
	}
	for _, v := range m {
		md, ok := index[v.name]
		if !ok {
			t.Errorf("no metric named %q", v.name)
			continue
		}
		v.validator(t, md)
	}
}

// histogramInt64Stats returns count, min, and max across all data points of a Histogram[int64] metric.
func histogramInt64Stats(hist metricdata.Histogram[int64]) (count uint64, min, max int64) {
	first := true
	for _, dp := range hist.DataPoints {
		count += dp.Count
		if minVal, valid := dp.Min.Value(); valid {
			if first || minVal < min {
				min = minVal
			}
		}
		if maxVal, valid := dp.Max.Value(); valid {
			if first || maxVal > max {
				max = maxVal
			}
			first = false
		}
	}
	return count, min, max
}

// countMeterValidator validates that a counter (or UpDownCounter) metric has the expected total value.
func countMeterValidator(name string, expectedCount int) *metricValidator {
	return &metricValidator{
		name: name,
		validator: func(t *testing.T, md metricdata.Metrics) {
			t.Helper()
			var total int64
			if sum, ok := md.Data.(metricdata.Sum[int64]); ok {
				for _, dp := range sum.DataPoints {
					total += dp.Value
				}
			}
			if total != int64(expectedCount) {
				t.Errorf("expected metric %q sum = %d, got %d", name, expectedCount, total)
			}
		},
	}
}

//lint:ignore U1000 // used in functional tests
func minCountMeterValidator(name string, minCount int) *metricValidator {
	return &metricValidator{
		name: name,
		validator: func(t *testing.T, md metricdata.Metrics) {
			t.Helper()
			var c int64
			if sum, ok := md.Data.(metricdata.Sum[int64]); ok {
				c += int64(len(sum.DataPoints))
			}
			if c < int64(minCount) {
				t.Errorf("Expected meter metric '%s' count >= %d, got %d", name, minCount, c)
			}
		},
	}
}

func countHistogramValidator(name string, expectedCount int) *metricValidator {
	return &metricValidator{
		name: name,
		validator: func(t *testing.T, md metricdata.Metrics) {
			t.Helper()
			hist, ok := md.Data.(metricdata.Histogram[int64])
			if !ok {
				t.Errorf("expected Histogram[int64] for metric %q, got %T", name, md.Data)
			}
			c, _, _ := histogramInt64Stats(hist)
			if c != uint64(expectedCount) {
				t.Errorf("expected metric %q count = %d, got %d", name, expectedCount, c)
			}
		},
	}
}

//lint:ignore U1000 // used in functional tests
func minCountHistogramValidator(name string, minCount int) *metricValidator {
	return &metricValidator{
		name: name,
		validator: func(t *testing.T, md metricdata.Metrics) {
			t.Helper()
			hist, ok := md.Data.(metricdata.Histogram[int64])
			if !ok {
				t.Errorf("expected Histogram[int64] for metric %q, got %T", name, md.Data)
			}
			c, _, _ := histogramInt64Stats(hist)
			if c < uint64(minCount) {
				t.Errorf("expected metric %q count >= %d, got %d", name, minCount, c)
			}
		},
	}
}

//lint:ignore U1000 // used in functional tests
func minMaxHistogramValidator(name string, expectedMin int, expectedMax int) *metricValidator {
	return &metricValidator{
		name: name,
		validator: func(t *testing.T, md metricdata.Metrics) {
			t.Helper()
			hist, ok := md.Data.(metricdata.Histogram[int64])
			if !ok {
				t.Errorf("Expected Histogram[int64] for metric %q, got %T", name, md.Data)
			}
			_, min, max := histogramInt64Stats(hist)
			if min != int64(expectedMin) {
				t.Errorf("Expected metric %q min = %d, got %d", name, expectedMin, min)
			}
			if max != int64(expectedMax) {
				t.Errorf("Expected metric %q max = %d, got %d", name, expectedMax, max)
			}
		},
	}
}

//lint:ignore U1000 // used in functional tests
func minValHistogramValidator(name string, minMin int) *metricValidator {
	return &metricValidator{
		name: name,
		validator: func(t *testing.T, md metricdata.Metrics) {
			t.Helper()
			hist, ok := md.Data.(metricdata.Histogram[int64])
			if !ok {
				t.Errorf("Expected Histogram[int64] for metric %q, got %T", name, md.Data)
			}
			_, min, _ := histogramInt64Stats(hist)
			if min < int64(minMin) {
				t.Errorf("Expected histogram metric '%s' min >= %d, got %d", name, minMin, min)
			}
		},
	}
}

//lint:ignore U1000 // used in functional tests
func maxValHistogramValidator(name string, maxMax int) *metricValidator {
	return &metricValidator{
		name: name,
		validator: func(t *testing.T, md metricdata.Metrics) {
			t.Helper()
			hist, ok := md.Data.(metricdata.Histogram[int64])
			if !ok {
				t.Errorf("Expected Histogram[int64] for metric %q, got %T", name, md.Data)
			}
			_, _, max := histogramInt64Stats(hist)
			if max > int64(maxMax) {
				t.Errorf("Expected histogram metric '%s' max <= %d, got %d", name, maxMax, max)
			}
		},
	}
}
