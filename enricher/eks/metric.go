package eks

import (
	"context"

	"github.com/canva/amazon-kinesis-streams-for-fluent-bit/enricher/mappings"
	"github.com/canva/amazon-kinesis-streams-for-fluent-bit/metricserver"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type EnricherMetric struct {
	meter metric.Meter

	outputRecordCount  metric.Int64Counter
	outputDroppedCount metric.Int64Counter
}

func WithMetricServer(ms *metricserver.MetricServer) EnricherConfiguration {
	return func(e *Enricher) error {
		meter := ms.GetMeter("github.com/canva/amazon-kinesis-streams-for-fluent-bit/enricher/eks")
		outputRecordCount, err := meter.Int64Counter("fluent_bit_output_record_count", metric.WithDescription("output record counter"))

		if err != nil {
			return err
		}

		outputDroppedCount, err := meter.Int64Counter("fluent_bit_output_record_dropped_count", metric.WithDescription("output dropped record counter where log or message does not exist"))

		if err != nil {
			return err
		}

		e.metric = &EnricherMetric{
			meter:              ms.GetMeter("github.com/canva/amazon-kinesis-streams-for-fluent-bit/enricher/eks"),
			outputRecordCount:  outputRecordCount,
			outputDroppedCount: outputDroppedCount,
		}

		return nil
	}
}

func (e *Enricher) AddRecordCount(record map[interface{}]interface{}, recordType int) {
	if e.metric == nil {
		return
	}

	var serviceName = inferServiceName(record, recordType)

	e.metric.outputRecordCount.Add(context.TODO(), 1, metric.WithAttributes(attribute.Key(mappings.RESOURCE_SERVICE_NAME).String(serviceName)))
}

func (e *Enricher) AddDropCount() {
	if e.metric == nil {
		return
	}

	e.metric.outputDroppedCount.Add(context.TODO(), 1)
}

// func (e *Enricher) AddRecordSize(record map[interface{}]interface{}, recordType int) {
// 	if e.metric == nil {
// 		return
// 	}

// 	var serviceName = inferServiceName(record, recordType)

// 	jsonStr, err := json.Marshal()
// 	if err != nil {
// 		logrus.Error(err)
// 	}

// 	fmt.Println(serviceName, len(jsonStr))

// 	// e.metric.outputSizseCount.Add(context.TODO(), int64(len(jsonStr)), metric.WithAttributes(attribute.Key(mappings.RESOURCE_SERVICE_NAME).String(serviceName)))
// }

func inferServiceName(record map[interface{}]interface{}, recordType int) string {
	var serviceName string

	switch recordType {
	case TYPE_APPLICATION:
		k8sPayload := record[mappings.KUBERNETES_RESOURCE_FIELD_NAME].(map[interface{}]interface{})
		labels, labelsExist := k8sPayload[mappings.KUBERNETES_LABELS_FIELD_NAME].(map[interface{}]interface{})
		if labelsExist {
			if val, ok := labels[mappings.KUBERNETES_LABELS_CANVA_COMPONENT]; ok {
				serviceName = val.(string)
			} else if val, ok := labels[mappings.KUBERNETES_LABELS_NAME]; ok {
				serviceName = val.(string)
			}
		} else {
			serviceName = k8sPayload[mappings.KUBERNETES_CONTAINER_NAME].(string)
		}
	}

	return serviceName
}
