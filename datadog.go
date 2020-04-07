package datadog

import (
	"context"
	"regexp"
	"strings"

	"github.com/DataDog/datadog-go/statsd"
	"go.opentelemetry.io/otel/api/core"
	export "go.opentelemetry.io/otel/sdk/export/metric"
	"go.opentelemetry.io/otel/sdk/export/metric/aggregator"
)

const (
	// DefaultStatsAddrUDP specifies the default protocol (UDP) and address
	// for the DogStatsD service.
	DefaultStatsAddrUDP = "localhost:8125"
)

// NewExporter exports to a datadog client
func NewExporter(opts Options) (export.Exporter, error) {
	endpoint := opts.StatsAddr
	if endpoint == "" {
		endpoint = DefaultStatsAddrUDP
	}
	client, err := statsd.New(endpoint)
	if err != nil {
		return nil, err
	}
	return &exporter{
		client: client,
		opts:   opts,
	}, nil
}

// Options contains options for configuring the exporter.
type Options struct {
	// Namespace specifies the namespaces to which metric keys are appended.
	Namespace string

	// StatsAddr specifies the host[:port] address for DogStatsD. It defaults
	// to localhost:8125.
	StatsAddr string

	// Tags specifies a set of global tags to attach to each metric.
	Tags []string
}

type exporter struct {
	opts   Options
	client *statsd.Client
}

const rate = 1

func (e *exporter) Export(ctx context.Context, cs export.CheckpointSet) error {
	return cs.ForEach(func(r export.Record) error {
		agg := r.Aggregator()
		name := sanitizeMetricName(e.opts.Namespace, r.Descriptor().Name())
		itr := r.Labels().Iter()
		tags := append([]string{}, e.opts.Tags...)
		for itr.Next() {
			label := itr.Label()
			tag := string(label.Key) + ":" + label.Value.Emit()
			tags = append(tags, tag)
		}
		switch agg := agg.(type) {
		case aggregator.MinMaxSumCount:
			type record struct {
				name string
				f    func() (core.Number, error)
			}
			recs := []record{
				{
					name: name + ".min",
					f:    agg.Min,
				},
				{
					name: name + ".max",
					f:    agg.Max,
				},
			}
			if dist, ok := agg.(aggregator.Distribution); ok {
				recs = append(recs,
					record{name: name + ".median", f: func() (core.Number, error) {
						return dist.Quantile(0.5)
					}},
					record{name: name + ".p95", f: func() (core.Number, error) {
						return dist.Quantile(0.95)
					}},
				)
			}
			for _, rec := range recs {
				val, err := rec.f()
				if err != nil {
					return err
				}
				e.client.Gauge(rec.name, metricValue(r.Descriptor().NumberKind(), val), tags, rate)
			}
		case aggregator.Sum:
			val, err := agg.Sum()
			if err != nil {
				return err
			}
			e.client.Gauge(name+".count", metricValue(r.Descriptor().NumberKind(), val), tags, rate)
		case aggregator.LastValue:
			val, _, err := agg.LastValue()
			if err != nil {
				return err
			}
			e.client.Gauge(name+".count", metricValue(r.Descriptor().NumberKind(), val), tags, rate)
		}
		return nil
	})
}

// sanitizeMetricName formats the custom namespace and view name to
// Datadog's metric naming convention
func sanitizeMetricName(namespace, name string) string {
	if namespace != "" {
		namespace = strings.Replace(namespace, " ", "", -1)
		return sanitizeString(namespace) + "." + sanitizeString(name)
	}
	return sanitizeString(name)
}

// regex pattern
var reg = regexp.MustCompile("[^a-zA-Z0-9]+")

// sanitizeString replaces all non-alphanumerical characters to underscore
func sanitizeString(str string) string {
	return reg.ReplaceAllString(str, "_")
}

func metricValue(kind core.NumberKind, number core.Number) float64 {
	switch kind {
	case core.Float64NumberKind:
		return number.AsFloat64()
	case core.Int64NumberKind:
		return float64(number.AsInt64())
	case core.Uint64NumberKind:
		return float64(number.AsUint64())
	}
	return float64(number)
}
