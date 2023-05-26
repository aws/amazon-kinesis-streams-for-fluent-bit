package eks

import (
	"testing"
	"time"

	"github.com/canva/amazon-kinesis-streams-for-fluent-bit/enricher/mappings"
	"github.com/stretchr/testify/assert"
)

func Test_NewEnricher(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		envs := map[string]string{
			mappings.ENV_ACCOUNT_ID:             DummyAccountId,
			mappings.ENV_ACCOUNT_NAME:           DummyAccountName,
			mappings.ENV_REGION:                 DummyRegion,
			mappings.ENV_ACCOUNT_GROUP_FUNCTION: DummyAccountGroupFunction,
			mappings.ENV_CLUSTER_NAME:           DummyClusterName,
			mappings.ENV_NODE_NAME:              DummyNodeName,
			mappings.ENV_PARTITION:              DummyPartition,
			mappings.ENV_ORGANISATION:           DummyOrganization,
			mappings.ENV_PLATFORM:               DummyPlatform,
			mappings.ENV_PROVIDER:               DummyProvider,
		}

		for env, val := range envs {
			t.Setenv(env, val)
		}

		enricher, err := NewEnricher()
		assert.NoError(t, err)
		assert.NotNil(t, enricher)

		t.Cleanup(func() {})
	})
	t.Run("Invalid", func(t *testing.T) {
		enricher, err := NewEnricher()

		assert.Nil(t, enricher)
		assert.Error(t, err)
	})
}

func Test_EnrichRecord(t *testing.T) {
	dummyLog := "hello world"
	defaultInputWithLog := map[interface{}]interface{}{
		mappings.LOG_FIELD_NAME: dummyLog,
		mappings.KUBERNETES_RESOURCE_FIELD_NAME: map[interface{}]interface{}{
			// default value, check if this isn't removed
			"key": "value",
		},
	}

	defaultEnricher := Enricher{
		CloudAccountId:            DummyAccountId,
		CloudAccountName:          DummyAccountName,
		CloudRegion:               DummyRegion,
		CloudPartition:            DummyPartition,
		CloudAccountGroupFunction: DummyAccountGroupFunction,
		K8sClusterName:            DummyClusterName,
		K8sNodeName:               DummyNodeName,
		CloudProvider:             DummyProvider,
		CloudPlatform:             DummyProvider,
		Organization:              DummyOrganization,
	}

	defaultExpectedWithLog := map[interface{}]interface{}{
		mappings.LOG_FIELD_NAME: dummyLog,
		mappings.RESOURCE_FIELD_NAME: map[interface{}]interface{}{
			mappings.RESOURCE_ACCOUNT_ID:             defaultEnricher.CloudAccountId,
			mappings.RESOURCE_ACCOUNT_NAME:           defaultEnricher.CloudAccountName,
			mappings.RESOURCE_REGION:                 defaultEnricher.CloudRegion,
			mappings.RESOURCE_PARTITION:              defaultEnricher.CloudPartition,
			mappings.RESOURCE_ACCOUNT_GROUP_FUNCTION: defaultEnricher.CloudAccountGroupFunction,
			mappings.RESOURCE_ORGANIZATION:           defaultEnricher.Organization,
			mappings.RESOURCE_PLATFORM:               defaultEnricher.CloudPlatform,
			mappings.RESOURCE_PROVIDER:               defaultEnricher.CloudProvider,
		},
		mappings.KUBERNETES_RESOURCE_FIELD_NAME: map[interface{}]interface{}{
			"key": "value",
			mappings.KUBERNETES_RESOURCE_CLUSTER_NAME: defaultEnricher.K8sClusterName,
			mappings.KUBERNETES_RESOURCE_NODE_NAME:    defaultEnricher.K8sNodeName,
		},
		mappings.OBSERVED_TIMESTAMP: ExpectedTime,
	}

	type TestCase struct {
		Test     string
		Enricher Enricher
		Input    map[interface{}]interface{}
		Expected map[interface{}]interface{}
	}

	testCases := []TestCase{
		{
			Test:     "Valid with log field",
			Enricher: defaultEnricher,
			Input:    defaultInputWithLog,
			Expected: defaultExpectedWithLog,
		},
		{
			Test:     "Valid with message field",
			Enricher: defaultEnricher,
			Input: func() map[interface{}]interface{} {
				input := copy(defaultInputWithLog)
				delete(input, mappings.LOG_FIELD_NAME)
				input[mappings.MESSAGE_FIELD_NAME] = "message"
				return input
			}(),
			Expected: func() map[interface{}]interface{} {
				expected := copy(defaultExpectedWithLog)
				delete(expected, mappings.LOG_FIELD_NAME)
				expected[mappings.MESSAGE_FIELD_NAME] = "message"
				return expected
			}(),
		},
		{
			Test:     "Drop log if log field and message is empty",
			Enricher: defaultEnricher,
			Input: func() map[interface{}]interface{} {
				input := copy(defaultInputWithLog)
				delete(input, mappings.LOG_FIELD_NAME)
				return input
			}(),
			Expected: nil,
		},
		{
			Test:     "Enrich placeholder service name if kubernetes field is empty",
			Enricher: defaultEnricher,
			Input: func() map[interface{}]interface{} {
				input := copy(defaultInputWithLog)
				delete(input, mappings.KUBERNETES_RESOURCE_FIELD_NAME)
				return input
			}(),
			Expected: func() map[interface{}]interface{} {
				expected := copy(defaultExpectedWithLog)
				expected[mappings.KUBERNETES_RESOURCE_FIELD_NAME] = map[interface{}]interface{}{
					mappings.KUBERNETES_CONTAINER_NAME:        mappings.PLACEHOLDER_MISSING_KUBERNETES_METADATA,
					mappings.KUBERNETES_RESOURCE_CLUSTER_NAME: defaultEnricher.K8sClusterName,
					mappings.KUBERNETES_RESOURCE_NODE_NAME:    defaultEnricher.K8sNodeName,
				}
				return expected
			}(),
		},
		{
			Test:     "Enrich service_name if log is an EKS host log",
			Enricher: defaultEnricher,
			Input: func() map[interface{}]interface{} {
				input := map[interface{}]interface{}{
					"boot_id":                    "03573569ccd4446688990194334a841c",
					"hostname":                   "localhost",
					"kernel_device":              "+pci_bus:0000:00",
					"kernel_subsystem":           "pci_bus",
					"machine_id":                 "ec2d184089fd93a22cc04a98df97cc6d",
					"message":                    "pci_bus 0000:00: resource 5 [io  0x0d00-0xffff window]",
					"priority":                   "7",
					"source_monotonic_timestamp": "576538",
					"syslog_facility":            "0",
					"syslog_identifier":          "kernel",
					"transport":                  "kernel",
					"udev_sysname":               "0000:00",
				}
				return input
			}(),
			Expected: func() map[interface{}]interface{} {
				expected := map[interface{}]interface{}{
					"boot_id":                    "03573569ccd4446688990194334a841c",
					"hostname":                   "localhost",
					"kernel_device":              "+pci_bus:0000:00",
					"kernel_subsystem":           "pci_bus",
					"machine_id":                 "ec2d184089fd93a22cc04a98df97cc6d",
					"message":                    "pci_bus 0000:00: resource 5 [io  0x0d00-0xffff window]",
					"priority":                   "7",
					"source_monotonic_timestamp": "576538",
					"syslog_facility":            "0",
					"syslog_identifier":          "kernel",
					"transport":                  "kernel",
					"udev_sysname":               "0000:00",
					mappings.OBSERVED_TIMESTAMP:  ExpectedTime,
				}
				expected[mappings.KUBERNETES_RESOURCE_FIELD_NAME] = map[interface{}]interface{}{
					mappings.KUBERNETES_RESOURCE_NODE_NAME:    defaultEnricher.K8sNodeName,
					mappings.KUBERNETES_RESOURCE_CLUSTER_NAME: defaultEnricher.K8sClusterName,
				}
				expected[mappings.RESOURCE_FIELD_NAME] = map[interface{}]interface{}{
					mappings.RESOURCE_ACCOUNT_ID:             defaultEnricher.CloudAccountId,
					mappings.RESOURCE_ACCOUNT_NAME:           defaultEnricher.CloudAccountName,
					mappings.RESOURCE_REGION:                 defaultEnricher.CloudRegion,
					mappings.RESOURCE_PARTITION:              defaultEnricher.CloudPartition,
					mappings.RESOURCE_ACCOUNT_GROUP_FUNCTION: defaultEnricher.CloudAccountGroupFunction,
					mappings.RESOURCE_ORGANIZATION:           defaultEnricher.Organization,
					mappings.RESOURCE_PLATFORM:               defaultEnricher.CloudPlatform,
					mappings.RESOURCE_PROVIDER:               defaultEnricher.CloudProvider,
					mappings.RESOURCE_SERVICE_NAME:           mappings.EKS_HOST_LOG_SERVICE_NAME,
				}
				return expected
			}(),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Test, func(t *testing.T) {
			actual := tc.Enricher.EnrichRecord(tc.Input, DummyTime)
			assert.Equal(t, tc.Expected, actual)
		})
	}
}

var (
	DummyAccountId            = "123123123"
	DummyAccountName          = "Account Name"
	DummyRegion               = "ap-southeast-1"
	DummyAccountGroupFunction = "general"
	DummyClusterName          = "Cluster Name"
	DummyNodeName             = "node_name"
	DummyPartition            = "aws"
	DummyOrganization         = "canva"
	DummyProvider             = "aws"
	DummyPlatform             = "eks"
	DummyTime                 = time.Date(2009, time.November, 10, 23, 7, 5, 432000000, time.UTC)
)

var (
	ExpectedTime = int64(1257894425432)
)

// Helper function to deep copy map.
func copy(m map[interface{}]interface{}) map[interface{}]interface{} {
	newMap := map[interface{}]interface{}{}

	for k, v := range m {
		vm, ok := v.(map[interface{}]interface{})
		if ok {
			newMap[k] = copy(vm)
		} else {
			newMap[k] = v
		}
	}

	return newMap
}
