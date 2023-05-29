package metrics

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

const (
	DEFAULT_PORT = 2112
)

type MetricsServer struct {
	port int
}

func New() *MetricsServer {
	return &MetricsServer{
		port: DEFAULT_PORT,
	}
}

func (m *MetricsServer) Start() {
	http.Handle("/metrics", promhttp.Handler())
	logrus.Fatal(http.ListenAndServe(fmt.Sprintf(":%v", m.port), nil))
}
