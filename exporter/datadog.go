package exporter

import (
	"context"
	"fmt"

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
func NewExporter(cfg Config, options ...statsd.Option) (*Exporter, error) {
	if cfg.StatsAddr == "" {
		cfg.StatsAddr = DefaultStatsAddrUDP
	}
	client, err := statsd.New(cfg.StatsAddr, options...)
	if err != nil {
		return nil, err
	}
	return &Exporter{
		client: client,
		cfg:    cfg,
	}, nil
}

// Options contains options for configuring the exporter.
type Config struct {
	// StatsAddr specifies the host[:port] address for DogStatsD. It defaults
	// to localhost:8125.
	StatsAddr string

	// UseDistribution uses a DataDog Distribution type instead of Histogram
	UseDistribution bool
}

// Exporter forwards metrics to a DataDog agent
type Exporter struct {
	cfg    Config
	client *statsd.Client
}

const rate = 1

// nolint:gocyclo
func (e *Exporter) Export(_ context.Context, cs export.CheckpointSet) error {
	return cs.ForEach(func(r export.Record) error {
		agg := r.Aggregator()
		name := r.Descriptor().Name()
		tags := extractTags(r)

		switch agg := agg.(type) {
		case aggregator.Points:
			numbers, err := agg.Points()
			if err != nil {
				return fmt.Errorf("error getting Points for %s: %w", name, err)
			}
			f := e.client.Histogram
			if e.cfg.UseDistribution {
				f = e.client.Distribution
			}
			for _, n := range numbers {
				err := f(name, metricValue(r.Descriptor().NumberKind(), n), tags, rate)
				if err != nil {
					return err
				}
			}
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
					return fmt.Errorf("error getting MinMaxSumCount value for %s: %w", name, err)
				}
				err = e.client.Gauge(rec.name, metricValue(r.Descriptor().NumberKind(), val), tags, rate)
				if err != nil {
					return err
				}
			}
		case aggregator.Sum:
			val, err := agg.Sum()
			if err != nil {
				return fmt.Errorf("error getting Sum value for %s: %w", name, err)
			}
			err = e.client.Count(name, val.AsInt64(), tags, rate)
			if err != nil {
				return err
			}
		case aggregator.LastValue:
			val, _, err := agg.LastValue()
			if err != nil {
				return fmt.Errorf("error getting LastValue for %s: %w", name, err)
			}
			err = e.client.Gauge(name, metricValue(r.Descriptor().NumberKind(), val), tags, rate)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func extractTags(r export.Record) []string {
	itr := r.Labels().Iter()
	tags := make([]string, itr.Len())
	for itr.Next() {
		i, label := itr.IndexedLabel()
		tags[i] = string(label.Key) + ":" + label.Value.Emit()
	}
	return tags
}

// Close cloess the underlying datadog client which flushes
// any pending buffers
func (e *Exporter) Close() error {
	return e.client.Close()
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
