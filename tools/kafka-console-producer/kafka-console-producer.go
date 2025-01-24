package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/tools/tls"
)

// goMetricsEncoder encodes OTEL metrics in a flat key-value format
// similar to go-metrics' WriteOnce output.
// Reference: https://github.com/rcrowley/go-metrics/blob/master/writer.go#L20
type goMetricsEncoder struct{}

func (e *goMetricsEncoder) Encode(v any) error {
	rm, ok := v.(*metricdata.ResourceMetrics)
	if !ok {
		return json.NewEncoder(os.Stderr).Encode(v)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			switch data := m.Data.(type) {
			case metricdata.Histogram[float64]:
				for _, dp := range data.DataPoints {
					prefix := metricPrefix(m.Name, dp.Attributes)
					fmt.Fprintf(os.Stdout, "histogram %s\n", prefix)
					fmt.Fprintf(os.Stdout, "  count: %d\n", dp.Count)
					fmt.Fprintf(os.Stdout, "  sum: %f\n", dp.Sum)
					if v, valid := dp.Min.Value(); valid {
						fmt.Fprintf(os.Stdout, "  min: %f\n", v)
					}
					if v, valid := dp.Max.Value(); valid {
						fmt.Fprintf(os.Stdout, "  max: %f\n", v)
					}
				}
			case metricdata.Histogram[int64]:
				for _, dp := range data.DataPoints {
					prefix := metricPrefix(m.Name, dp.Attributes)
					fmt.Fprintf(os.Stdout, "histogram %s\n", prefix)
					fmt.Fprintf(os.Stdout, "  count: %d\n", dp.Count)
					fmt.Fprintf(os.Stdout, "  sum: %d\n", dp.Sum)
					if v, valid := dp.Min.Value(); valid {
						fmt.Fprintf(os.Stdout, "  min: %d\n", v)
					}
					if v, valid := dp.Max.Value(); valid {
						fmt.Fprintf(os.Stdout, "  max: %d\n", v)
					}
				}
			case metricdata.Sum[float64]:
				for _, dp := range data.DataPoints {
					prefix := metricPrefix(m.Name, dp.Attributes)
					fmt.Fprintf(os.Stdout, "sum %s\n", prefix)
					fmt.Fprintf(os.Stdout, "  value: %f\n", dp.Value)
				}
			case metricdata.Sum[int64]:
				for _, dp := range data.DataPoints {
					prefix := metricPrefix(m.Name, dp.Attributes)
					fmt.Fprintf(os.Stdout, "sum %s\n", prefix)
					fmt.Fprintf(os.Stdout, "  value: %d\n", dp.Value)
				}
			case metricdata.Gauge[float64]:
				for _, dp := range data.DataPoints {
					prefix := metricPrefix(m.Name, dp.Attributes)
					fmt.Fprintf(os.Stdout, "gauge %s\n", prefix)
					fmt.Fprintf(os.Stdout, "  value: %f\n", dp.Value)
				}
			case metricdata.Gauge[int64]:
				for _, dp := range data.DataPoints {
					prefix := metricPrefix(m.Name, dp.Attributes)
					fmt.Fprintf(os.Stdout, "gauge %s\n", prefix)
					fmt.Fprintf(os.Stdout, "  value: %d\n", dp.Value)
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

var (
	brokerList    = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The comma separated list of brokers in the Kafka cluster. You can also set the KAFKA_PEERS environment variable")
	headers       = flag.String("headers", "", "The headers of the message to produce. Example: -headers=foo:bar,bar:foo")
	topic         = flag.String("topic", "", "REQUIRED: the topic to produce to")
	key           = flag.String("key", "", "The key of the message to produce. Can be empty.")
	value         = flag.String("value", "", "REQUIRED: the value of the message to produce. You can also provide the value on stdin.")
	partitioner   = flag.String("partitioner", "", "The partitioning scheme to use. Can be `hash`, `manual`, or `random`")
	partition     = flag.Int("partition", -1, "The partition to produce to.")
	verbose       = flag.Bool("verbose", false, "Turn on sarama logging to stderr")
	showMetrics   = flag.Bool("metrics", false, "Output metrics on successful publish to stderr")
	silent        = flag.Bool("silent", false, "Turn off printing the message's topic, partition, and offset to stdout")
	tlsEnabled    = flag.Bool("tls-enabled", false, "Whether to enable TLS")
	tlsSkipVerify = flag.Bool("tls-skip-verify", false, "Whether skip TLS server cert verification")
	tlsClientCert = flag.String("tls-client-cert", "", "Client cert for client authentication (use with -tls-enabled and -tls-client-key)")
	tlsClientKey  = flag.String("tls-client-key", "", "Client key for client authentication (use with tls-enabled and -tls-client-cert)")

	logger = log.New(os.Stderr, "", log.LstdFlags)
)

func main() {
	flag.Parse()

	if *brokerList == "" {
		printUsageErrorAndExit("no -brokers specified. Alternatively, set the KAFKA_PEERS environment variable")
	}

	if *topic == "" {
		printUsageErrorAndExit("no -topic specified")
	}

	if *verbose {
		sarama.Logger = logger
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	if *tlsEnabled {
		tlsConfig, err := tls.NewConfig(*tlsClientCert, *tlsClientKey)
		if err != nil {
			printErrorAndExit(69, "Failed to create TLS config: %s", err)
		}

		config.Net.TLS.Enable = true
		config.Net.TLS.Config = tlsConfig
		config.Net.TLS.Config.InsecureSkipVerify = *tlsSkipVerify
	}

	switch *partitioner {
	case "":
		if *partition >= 0 {
			config.Producer.Partitioner = sarama.NewManualPartitioner
		} else {
			config.Producer.Partitioner = sarama.NewHashPartitioner
		}
	case "hash":
		config.Producer.Partitioner = sarama.NewHashPartitioner
	case "random":
		config.Producer.Partitioner = sarama.NewRandomPartitioner
	case "manual":
		config.Producer.Partitioner = sarama.NewManualPartitioner
		if *partition == -1 {
			printUsageErrorAndExit("-partition is required when partitioning manually")
		}
	default:
		printUsageErrorAndExit(fmt.Sprintf("Partitioner %s not supported.", *partitioner))
	}

	var reader *metric.ManualReader
	if *showMetrics {
		reader = metric.NewManualReader()
		provider := metric.NewMeterProvider(metric.WithReader(reader))
		defer provider.Shutdown(context.Background())
		otel.SetMeterProvider(provider)
	}

	message := &sarama.ProducerMessage{Topic: *topic, Partition: int32(*partition)}

	if *key != "" {
		message.Key = sarama.StringEncoder(*key)
	}

	if *value != "" {
		message.Value = sarama.StringEncoder(*value)
	} else if stdinAvailable() {
		bytes, err := io.ReadAll(os.Stdin)
		if err != nil {
			printErrorAndExit(66, "Failed to read data from the standard input: %s", err)
		}
		message.Value = sarama.ByteEncoder(bytes)
	} else {
		printUsageErrorAndExit("-value is required, or you have to provide the value on stdin")
	}

	if *headers != "" {
		var hdrs []sarama.RecordHeader
		for h := range strings.SplitSeq(*headers, ",") {
			if header := strings.Split(h, ":"); len(header) != 2 {
				printUsageErrorAndExit("-header should be key:value. Example: -headers=foo:bar,bar:foo")
			} else {
				hdrs = append(hdrs, sarama.RecordHeader{
					Key:   []byte(header[0]),
					Value: []byte(header[1]),
				})
			}
		}

		if len(hdrs) != 0 {
			message.Headers = hdrs
		}
	}

	producer, err := sarama.NewSyncProducer(strings.Split(*brokerList, ","), config)
	if err != nil {
		printErrorAndExit(69, "Failed to open Kafka producer: %s", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			logger.Println("Failed to close Kafka producer cleanly:", err)
		}
	}()

	partition, offset, err := producer.SendMessage(message)
	if err != nil {
		printErrorAndExit(69, "Failed to produce message: %s", err)
	} else if !*silent {
		fmt.Printf("topic=%s\tpartition=%d\toffset=%d\n", *topic, partition, offset)
	}
	if reader != nil {
		var rm metricdata.ResourceMetrics
		_ = reader.Collect(context.Background(), &rm)
		exp, _ := stdoutmetric.New(stdoutmetric.WithPrettyPrint(), stdoutmetric.WithEncoder(&goMetricsEncoder{}))
		_ = exp.Export(context.Background(), &rm)
	}
}

func printErrorAndExit(code int, format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	os.Exit(code)
}

func printUsageErrorAndExit(message string) {
	fmt.Fprintln(os.Stderr, "ERROR:", message)
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Available command line options:")
	flag.PrintDefaults()
	os.Exit(64)
}

func stdinAvailable() bool {
	stat, _ := os.Stdin.Stat()
	return (stat.Mode() & os.ModeCharDevice) == 0
}
