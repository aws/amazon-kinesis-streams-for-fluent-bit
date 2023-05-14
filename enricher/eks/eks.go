package eks

import (
	"time"

	"github.com/caarlos0/env/v7"
	"github.com/canva/amazon-kinesis-streams-for-fluent-bit/enricher"
	"github.com/canva/amazon-kinesis-streams-for-fluent-bit/enricher/mappings"
)

type Enricher struct {
	CloudAccountId            string `env:"CLOUD_ACCOUNT_ID,required"`
	CloudAccountName          string `env:"CLOUD_ACCOUNT_NAME,required"`
	CloudRegion               string `env:"CLOUD_REGION,required"`
	K8sClusterName            string `env:"K8S_CLUSTER_NAME,required"`
	CloudPartition            string `env:"CLOUD_PARTITION,required"`
	CloudAccountGroupFunction string `env:"CLOUD_ACCOUNT_GROUP_FUNCTION,required"`
	Organization              string `env:"ORGANIZATION,required"`
	CloudProvider             string `env:"CLOUD_PROVIDER,required"`
	CloudPlatform             string `env:"CLOUD_PLATFORM,required"`
}

// NewEnricher returns a enricher with env vars being parsed.
// These env vars are derived from mappings.go.
func NewEnricher() (*Enricher, error) {
	enricher := Enricher{}
	if err := env.Parse(&enricher); err != nil {
		return nil, err
	}

	return &enricher, nil
}

var _ enricher.IEnricher = (*Enricher)(nil)

func (e Enricher) EnrichRecord(r map[interface{}]interface{}, t time.Time) map[interface{}]interface{} {
	// Drop log if "log" field is empty
	if r[mappings.LOG_FIELD_NAME] == nil {
		return nil
	}

	// add resource attributes
	r[mappings.RESOURCE_FIELD_NAME] = map[interface{}]interface{}{
		mappings.RESOURCE_ACCOUNT_ID:             e.CloudAccountId,
		mappings.RESOURCE_ACCOUNT_NAME:           e.CloudAccountName,
		mappings.RESOURCE_ACCOUNT_GROUP_FUNCTION: e.CloudAccountGroupFunction,
		mappings.RESOURCE_PARTITION:              e.CloudPartition,
		mappings.RESOURCE_REGION:                 e.CloudRegion,
		mappings.RESOURCE_ORGANIZATION:           e.Organization,
		mappings.RESOURCE_PLATFORM:               e.CloudPlatform,
		mappings.RESOURCE_PROVIDER:               e.CloudProvider,
	}

	r[mappings.OBSERVED_TIMESTAMP] = t.UnixMilli()

	// If Fluentbit has failed to enrich k8s metadata on the log, we insert a placeholder value for the kubernetes.service_name
	// https://docs.google.com/document/d/1vRCUKMeo6ypnAq34iwQN7LtDsXxmlj0aYEfRofwV7A4/edit
	if _, ok := r[mappings.KUBERNETES_RESOURCE_FIELD_NAME]; !ok {
		r[mappings.KUBERNETES_RESOURCE_FIELD_NAME] = map[interface{}]interface{}{
			mappings.KUBERNETES_CONTAINER_NAME: mappings.PLACEHOLDER_MISSING_KUBERNETES_METADATA,
		}
	}

	r[mappings.KUBERNETES_RESOURCE_FIELD_NAME].(map[interface{}]interface{})[mappings.KUBERNETES_RESOURCE_CLUSTER_NAME] = e.K8sClusterName

	return r
}
