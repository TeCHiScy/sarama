package sarama

import (
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

func brokerAttributes(b *Broker) []attribute.KeyValue {
	return []attribute.KeyValue{attribute.String("broker", b.Addr())}
}

func topicAttributes(topic string) []attribute.KeyValue {
	return []attribute.KeyValue{attribute.String("topic", topic)}
}

func topicPartitionAttributes(topic string, partition int32) []attribute.KeyValue {
	return append(topicAttributes(topic), attribute.Int64("partition", int64(partition)))
}

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

func prefixedName(name string) string {
	return "sarama_" + name
}

func realMetrics(meter metric.Meter) *Metrics {
	consumerFetchs, _ := meter.Int64Counter(prefixedName("consumer_fetchs_total"))
	incomingBytes, _ := meter.Int64Counter(prefixedName("incoming_bytes_total"))
	requests, _ := meter.Int64Counter(prefixedName("requests_total"))
	requestSize, _ := meter.Int64Histogram(prefixedName("request_size"))
	requestLatency, _ := meter.Int64Histogram(prefixedName("request_latency"), metric.WithUnit("ms"))
	outgoingBytes, _ := meter.Int64Counter(prefixedName("outgoing_bytes_total"))
	responses, _ := meter.Int64Counter(prefixedName("responses_total"))
	responseSize, _ := meter.Int64Histogram(prefixedName("response_size"))
	requestsInFlight, _ := meter.Int64UpDownCounter(prefixedName("requests_in_flight"))
	protocolRequests, _ := meter.Int64Counter(prefixedName("protocol_requests_total"))
	throttleTime, _ := meter.Int64Histogram(prefixedName("throttle_time"), metric.WithUnit("ms"))

	batchSize, _ := meter.Int64Histogram(prefixedName("batch_size"))
	compressionRatio, _ := meter.Int64Histogram(prefixedName("compression_ratio"))
	recordSends, _ := meter.Int64Counter(prefixedName("record_sends"))
	recordsPerRequest, _ := meter.Int64Histogram(prefixedName("records_per_request"))
	consumerFetchResponseSize, _ := meter.Int64Histogram(prefixedName("consumer_fetch_response_size"))

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
