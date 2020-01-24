package kinesis

import (
	"encoding/json"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/aws/amazon-kinesis-firehose-for-fluent-bit/plugins"
	"github.com/aws/amazon-kinesis-streams-for-fluent-bit/kinesis/mock_kinesis"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	fluentbit "github.com/fluent/fluent-bit-go/output"
	"github.com/golang/mock/gomock"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

// newMockOutputPlugin creates an mock OutputPlugin object
func newMockOutputPlugin(client *mock_kinesis.MockPutRecordsClient) (*OutputPlugin, error) {
	records := make([]*kinesis.PutRecordsRequestEntry, 0, 500)

	timer, _ := plugins.NewTimeout(func(d time.Duration) {
		logrus.Errorf("[kinesis] timeout threshold reached: Failed to send logs for %v", d)
		logrus.Errorf("[kinesis] Quitting Fluent Bit")
		os.Exit(1)
	})

	seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
	b := make([]byte, 8)
	random := &random{
		seededRandom: seededRand,
		buffer:       b,
	}

	return &OutputPlugin{
		stream:                       "stream",
		client:                       client,
		records:                      records,
		dataKeys:                     "",
		partitionKey:                 "",
		lastInvalidPartitionKeyIndex: -1,
		backoff:                      plugins.NewBackoff(),
		timer:                        timer,
		PluginID:                     0,
		random:                       random,
		replaceDots:                  true,
	}, nil
}

// Test cases for TestStringOrByteArray
var testCases = []struct {
	input  interface{}
	output string
}{
	{"testString", "testString"},
	{35344, ""},
	{[]byte{'b', 'y', 't', 'e'}, "byte"},
	{nil, ""},
}

func TestStringOrByteArray(t *testing.T) {
	for _, testCase := range testCases {
		result := stringOrByteArray(testCase.input)
		if result != testCase.output {
			t.Errorf("[Test Failed] Expeced: %s, Returned: %s", testCase.output, result)
		}
	}
}

func TestAddRecord(t *testing.T) {
	record := map[interface{}]interface{}{
		"testkey": []byte("test value"),
	}

	outputPlugin, _ := newMockOutputPlugin(nil)

	retCode := outputPlugin.AddRecord(record)
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to be FLB_OK")
	assert.Len(t, outputPlugin.records, 1, "Expected output to contain 1 record")
}

func TestAddRecordAndFlush(t *testing.T) {
	record := map[interface{}]interface{}{
		"testkey": []byte("test value"),
	}

	ctrl := gomock.NewController(t)
	mockKinesis := mock_kinesis.NewMockPutRecordsClient(ctrl)

	mockKinesis.EXPECT().PutRecords(gomock.Any()).Return(&kinesis.PutRecordsOutput{
		FailedRecordCount: aws.Int64(0),
	}, nil)

	outputPlugin, _ := newMockOutputPlugin(mockKinesis)

	retCode := outputPlugin.AddRecord(record)
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to be FLB_OK")

	err := outputPlugin.Flush()
	assert.NoError(t, err, "Unexpected error calling flush")
}

func TestDotReplace(t *testing.T) {
	record := map[interface{}]interface{}{
		"testkey": []byte("test value"),
		"kubernetes": map[interface{}]interface{}{
			"app":                    []byte("test app label"),
			"app.kubernetes.io/name": []byte("test key with dots"),
		},
	}

	outputPlugin, _ := newMockOutputPlugin(nil)

	retCode := outputPlugin.AddRecord(record)
	assert.Equal(t, retCode, fluentbit.FLB_OK, "Expected return code to be FLB_OK")
	assert.Len(t, outputPlugin.records, 1, "Expected output to contain 1 record")

	data := outputPlugin.records[0].Data

	var log map[string]map[string]interface{}
	json.Unmarshal(data, &log)

	assert.Equal(t, "test app label", log["kubernetes"]["app"])
	assert.Equal(t, "test key with dots", log["kubernetes"]["app_kubernetes_io/name"])
}
