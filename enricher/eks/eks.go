package eks

import (
	"time"

	"github.com/caarlos0/env/v7"
	"github.com/canva/amazon-kinesis-streams-for-fluent-bit/enricher"
	"github.com/canva/amazon-kinesis-streams-for-fluent-bit/enricher/mappings"
)

type Enricher struct {
	// AWS Account ID
	AccountId string `env:"CANVA_AWS_ACCOUNT,required"`
	// Canva Account Group Function
	CanvaAccountFunction string `env:"CANVA_ACCOUNT_FUNCTION,required"`
}

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
	if r["log"] == nil {
		return nil
	}

	// add resource attributes
	r["resource"] = map[interface{}]interface{}{
		mappings.RESOURCE_CLOUD_ACCOUNT_ID: e.AccountId,
		mappings.RESOURCE_ACCOUNT_GROUP:    e.CanvaAccountFunction,
	}

	r[mappings.OBSERVED_TIMESTAMP] = t.UnixMilli()

	return r
}
