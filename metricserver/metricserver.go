package metricserver

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdk "go.opentelemetry.io/otel/sdk/metric"
)

const (
	DEFAULT_PORT = 2112
)

type MetricServerConfiguration func(*MetricServer) error

type MetricServer struct {
	port int

	meterProvider *sdk.MeterProvider
}

func New(cfgs ...MetricServerConfiguration) (*MetricServer, error) {
	exporter, err := prometheus.New()
	if err != nil {
		return nil, err
	}
	provider := sdk.NewMeterProvider(sdk.WithReader(exporter))

	ms := &MetricServer{
		port:          DEFAULT_PORT,
		meterProvider: provider,
	}

	for _, cfg := range cfgs {
		err := cfg(ms)

		if err != nil {
			return nil, err
		}
	}

	return ms, nil
}

func WithPort(Port int) MetricServerConfiguration {
	return func(ms *MetricServer) error {
		ms.port = Port

		return nil
	}
}

func (m *MetricServer) Start() {
	http.Handle("/metrics", promhttp.Handler())
	logrus.Infof("Started metric server on port %v", m.port)
	logrus.Fatal(http.ListenAndServe(fmt.Sprintf(":%v", m.port), nil))
}

func (m *MetricServer) GetMeter(scope string) metric.Meter {
	meter := m.meterProvider.Meter(scope)
	return meter
}
