package aggregate

import (
	"crypto/md5"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

var (
	// Magic number for KCL aggregated records.  See this for details:  https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md
	kclMagicNumber    = []byte{0xF3, 0x89, 0x9A, 0xC2}
	kclMagicNumberLen = len(kclMagicNumber)
)

const (
	maximumRecordSize       = 1024 * 1024 // 1 MB
	defaultMaxAggRecordSize = 20 * 1024   // 20K
	pKeyIdxSize             = 8
	aggProtobufBytes        = 2 // Marshalling the data into protobuf adds an additional 2 bytes.
)

// Aggregator kinesis aggregator
type Aggregator struct {
	partitionKeys    map[string]uint64
	records          []*Record
	aggSize          int // Size of both records, and partitionKeys in bytes
	maxAggRecordSize int
}

// NewAggregator create a new aggregator
func NewAggregator() *Aggregator {

	return &Aggregator{
		partitionKeys:    make(map[string]uint64, 0),
		records:          make([]*Record, 0),
		maxAggRecordSize: defaultMaxAggRecordSize,
	}
}

// AddRecord to the aggregate buffer.
// Will return a kinesis PutRecordsRequest once buffer is full, or if the data exceeds the aggregate limit.
func (a *Aggregator) AddRecord(partitionKey string, data []byte) (entry *kinesis.PutRecordsRequestEntry, err error) {

	partitionKeySize := len([]byte(partitionKey))
	if partitionKeySize < 1 {
		return nil, fmt.Errorf("Invalid partition key provided")
	}

	dataSize := len(data)

	// If this is a very large record, then don't aggregate it.
	if dataSize >= a.maxAggRecordSize {
		return &kinesis.PutRecordsRequestEntry{
			Data:         data,
			PartitionKey: aws.String(partitionKey),
		}, nil
	}

	if a.getSize()+dataSize+partitionKeySize+pKeyIdxSize >= maximumRecordSize {
		// Aggregate records, and return
		entry, err = a.AggregateRecords()
		if err != nil {
			return entry, err
		}
	}

	// Add new record, and update aggSize
	partitionKeyIndex := a.addPartitionKey(partitionKey)

	a.records = append(a.records, &Record{
		Data:              data,
		PartitionKeyIndex: &partitionKeyIndex,
	})

	a.aggSize += dataSize + pKeyIdxSize

	return entry, err
}

// AggregateRecords will flush proto-buffered records into a put request
func (a *Aggregator) AggregateRecords() (entry *kinesis.PutRecordsRequestEntry, err error) {

	if len(a.records) == 0 {
		return nil, nil
	}

	pkeys := a.getPartitionKeys()

	agg := &AggregatedRecord{
		PartitionKeyTable: pkeys,
		Records:           a.records,
	}

	protoBufData, err := proto.Marshal(agg)
	if err != nil {
		logrus.Errorf("Failed to encode record: %v", err)
		return nil, err
	}

	md5Sum := md5.New()
	md5Sum.Write(protoBufData)
	md5CheckSum := md5Sum.Sum(nil)

	kclData := append(kclMagicNumber, protoBufData...)
	kclData = append(kclData, md5CheckSum...)

	logrus.Debugf("[kinesis ] Aggregated (%d) records of size (%d) with total size (%d), partition key (%s)\n", len(a.records), a.getSize(), len(kclData), pkeys[0])

	// Clear buffer if aggregation didn't fail
	a.clearBuffer()

	return &kinesis.PutRecordsRequestEntry{
		Data:         kclData,
		PartitionKey: aws.String(pkeys[0]),
	}, nil
}

// GetRecordCount gets number of buffered records
func (a *Aggregator) GetRecordCount() int {
	return len(a.records)
}

func (a *Aggregator) addPartitionKey(partitionKey string) uint64 {

	if idx, ok := a.partitionKeys[partitionKey]; ok {
		return idx
	}

	idx := uint64(len(a.partitionKeys))
	a.partitionKeys[partitionKey] = idx
	a.aggSize += len([]byte(partitionKey))
	return idx
}

func (a *Aggregator) getPartitionKeys() []string {
	keys := make([]string, 0)
	for pk := range a.partitionKeys {
		keys = append(keys, pk)
	}
	return keys
}

// getSize of protobuf records, partitionKeys, magicNumber, and md5sum in bytes
func (a *Aggregator) getSize() int {
	return a.aggSize + kclMagicNumberLen + md5.Size + aggProtobufBytes
}

func (a *Aggregator) clearBuffer() {
	a.partitionKeys = make(map[string]uint64, 0)
	a.records = make([]*Record, 0)
	a.aggSize = 0
}
