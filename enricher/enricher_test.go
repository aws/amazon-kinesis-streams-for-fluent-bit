package enricher_test

import (
	"testing"
	"time"

	"github.com/canva/amazon-kinesis-streams-for-fluent-bit/enricher"
	"github.com/stretchr/testify/assert"
)

func TestEnrichRecord(t *testing.T) {
	var cases = []struct {
		Name     string
		Enabled  bool
		Enricher enricher.IEnricher
		Input    map[interface{}]interface{}
		Expected map[interface{}]interface{}
	}{
		{
			Name:     "Disabled",
			Enabled:  false,
			Enricher: &DummyEnricher{},
			Input: map[interface{}]interface{}{
				"message": "hello world",
			},
			Expected: map[interface{}]interface{}{
				"message": "hello world",
			},
		},
		{
			Name:     "Enabled",
			Enabled:  true,
			Enricher: &DummyEnricher{},
			Input: map[interface{}]interface{}{
				"message": "hello world",
			},
			Expected: DummyRecord,
		},
	}

	for _, v := range cases {
		t.Run(v.Name, func(t *testing.T) {
			en := enricher.NewEnricher(v.Enabled, v.Enricher)

			actual := en.EnrichRecord(v.Input, DummyTime)

			assert.Equal(t, v.Expected, actual)
		})
	}
}

type DummyEnricher struct{}

func (d DummyEnricher) EnrichRecord(_ map[interface{}]interface{}, _ time.Time) map[interface{}]interface{} {
	return DummyRecord
}

var DummyRecord = map[interface{}]interface{}{
	"message": "I am enriched",
}

var DummyTime = time.Now()
