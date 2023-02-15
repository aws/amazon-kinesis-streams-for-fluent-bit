package enricher

import "time"

type IEnricher interface {
	EnrichRecord(r map[interface{}]interface{}, t time.Time) map[interface{}]interface{}
}

type Enricher struct {
	Enable bool

	enricher IEnricher
}

var _ IEnricher = (*Enricher)(nil)

func NewEnricher(enable bool, enricher IEnricher) *Enricher {
	return &Enricher{
		Enable:   enable,
		enricher: enricher,
	}
}

func (e *Enricher) EnrichRecord(r map[interface{}]interface{}, t time.Time) map[interface{}]interface{} {
	if !e.Enable {
		return r
	}

	return e.enricher.EnrichRecord(r, t)
}
