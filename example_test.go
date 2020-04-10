package datadog

import (
	"github.com/DataDog/datadog-go/statsd"
	"go.opentelemetry.io/otel/api/global"
	export "go.opentelemetry.io/otel/sdk/export/metric"
	"go.opentelemetry.io/otel/sdk/metric/batcher/ungrouped"
	"go.opentelemetry.io/otel/sdk/metric/controller/push"
)

func ExampleExporter() {
	exporter, _ := NewExporter(
		Config{
			StatsAddr:       DefaultStatsAddrUDP,
			UseDistribution: false,
		},
		statsd.WithoutTelemetry(),
		statsd.WithNamespace("crl_"),
		statsd.WithTags([]string{
			"example_tag:example_value",
		}),
	)

	selector := NewWithDataDogMeasure()
	batcher := ungrouped.New(selector, export.NewDefaultLabelEncoder(), false)
	pusher := push.New(batcher, exporter,1)
	pusher.Start()
	global.SetMeterProvider(pusher)
	defer pusher.Stop()
}
