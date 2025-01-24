package sarama

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

func brokerAttributes(b *Broker) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.Int64("broker_id", int64(b.ID())),
		attribute.String("broker_addr", b.Addr()),
		attribute.String("broker_rack", b.Rack()),
	}
}

func topicAttributes(topic string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("topic", topic),
	}
}

func topicPartitionAttributes(topic string, partitionID int32) []attribute.KeyValue {
	return append(topicAttributes(topic), attribute.Int64("partition_id", int64(partitionID)))
}

// TODO getOrRegisterHistogram
// TODO metrics.NewExpDecaySample(metricsReservoirSize, metricsAlphaFactor)

type Metrics struct {
	attributes []attribute.KeyValue

	consumerFetchs   metric.Int64Counter
	incomingBytes    metric.Int64Counter
	requests         metric.Int64Counter
	requestSize      metric.Int64Histogram
	requestLatency   metric.Int64Histogram
	outgoingBytes    metric.Int64Counter
	responses        metric.Int64Counter
	responseSize     metric.Int64Histogram
	requestsInFlight metric.Int64UpDownCounter
	protocolRequests metric.Int64Counter
	throttleTime     metric.Int64Histogram

	batchSize                 metric.Int64Histogram
	compressionRatio          metric.Int64Histogram
	recordSends               metric.Int64Counter
	recordsPerRequest         metric.Int64Histogram
	consumerFetchResponseSize metric.Int64Histogram

	broker *Broker
}

func (m *Metrics) withTopic(topic string) []attribute.KeyValue {
	return append(topicAttributes(topic), m.attributes...)
}

func (m *Metrics) withTopicPartition(topic string, partitionID int32) []attribute.KeyValue {
	return append(topicPartitionAttributes(topic, partitionID), m.attributes...)
}

func realMetrics(meter metric.Meter) *Metrics {
	consumerFetchs, _ := meter.Int64Counter("consumer_fetchs_total")
	incomingBytes, _ := meter.Int64Counter("incoming_bytes_total")
	requests, _ := meter.Int64Counter("requests_total")
	requestSize, _ := meter.Int64Histogram("request_size")
	requestLatency, _ := meter.Int64Histogram("request_latency_in_ms", metric.WithUnit("ms"))
	outgoingBytes, _ := meter.Int64Counter("outgoing_bytes_total")
	responses, _ := meter.Int64Counter("responses_total")
	responseSize, _ := meter.Int64Histogram("response_size")
	requestsInFlight, _ := meter.Int64UpDownCounter("requests_in_flight")
	protocolRequests, _ := meter.Int64Counter("protocol_requests_total")
	throttleTime, _ := meter.Int64Histogram("throttle_time_in_ms", metric.WithUnit("ms"))

	batchSize, _ := meter.Int64Histogram("batch_size")
	compressionRatio, _ := meter.Int64Histogram("compression_ratio")
	recordSends, _ := meter.Int64Counter("record_sends")
	recordsPerRequest, _ := meter.Int64Histogram("records_per_request")
	consumerFetchResponseSize, _ := meter.Int64Histogram("consumer_fetch_response_size")

	return &Metrics{
		consumerFetchs:   consumerFetchs,
		incomingBytes:    incomingBytes,
		requests:         requests,
		requestSize:      requestSize,
		requestLatency:   requestLatency,
		outgoingBytes:    outgoingBytes,
		responses:        responses,
		responseSize:     responseSize,
		requestsInFlight: requestsInFlight,
		protocolRequests: protocolRequests,
		throttleTime:     throttleTime,

		batchSize:                 batchSize,
		compressionRatio:          compressionRatio,
		recordSends:               recordSends,
		recordsPerRequest:         recordsPerRequest,
		consumerFetchResponseSize: consumerFetchResponseSize,
	}
}

// SetupMetrics sets up the metrics for the Sarama client.
func noopMetrics(meter metric.Meter) *Metrics {
	return &Metrics{
		consumerFetchs:   noop.Int64Counter{},
		incomingBytes:    noop.Int64Counter{},
		requests:         noop.Int64Counter{},
		requestSize:      noop.Int64Histogram{},
		requestLatency:   noop.Int64Histogram{},
		outgoingBytes:    noop.Int64Counter{},
		responses:        noop.Int64Counter{},
		responseSize:     noop.Int64Histogram{},
		requestsInFlight: noop.Int64UpDownCounter{},
		protocolRequests: noop.Int64Counter{},
		throttleTime:     noop.Int64Histogram{},

		batchSize:                 noop.Int64Histogram{},
		compressionRatio:          noop.Int64Histogram{},
		recordSends:               noop.Int64Counter{},
		recordsPerRequest:         noop.Int64Histogram{},
		consumerFetchResponseSize: noop.Int64Histogram{},
	}
}
