package datadog

import (
	"go.opentelemetry.io/otel/api/metric"
	export "go.opentelemetry.io/otel/sdk/export/metric"
	"go.opentelemetry.io/otel/sdk/metric/aggregator/array"
	"go.opentelemetry.io/otel/sdk/metric/aggregator/lastvalue"
	"go.opentelemetry.io/otel/sdk/metric/aggregator/sum"
)

type (
	selectorDataDog struct{}
)

func NewWithDataDogMeasure() export.AggregationSelector {
	return selectorDataDog{}
}

func (selectorDataDog) AggregatorFor(descriptor *metric.Descriptor) export.Aggregator {
	switch descriptor.MetricKind() {
	case metric.ObserverKind:
		return lastvalue.New()
	case metric.MeasureKind:
		return array.New()
	default:
		return sum.New()
	}
}
