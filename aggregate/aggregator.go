package aggregate

import (
	"crypto/md5"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/canva/amazon-kinesis-streams-for-fluent-bit/compress"
	"github.com/canva/amazon-kinesis-streams-for-fluent-bit/util"
)

var (
	// Magic number for KCL aggregated records.  See this for details:  https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md
	kclMagicNumber    = []byte{0xF3, 0x89, 0x9A, 0xC2}
	kclMagicNumberLen = len(kclMagicNumber)
)

const (
	defaultMaximumRecordSize = 1024 * 1024 // 1 MB
	defaultMaxAggRecordSize  = 20 * 1024   // 20K
	initialAggRecordSize     = 0
	fieldNumberSize          = 1 // All field numbers are below 16, meaning they will only take up 1 byte
)

// Aggregator kinesis aggregator
type Aggregator struct {
	partitionKeys     map[string]uint64
	records           []*Record
	aggSize           int // Size of both records, and partitionKeys in bytes
	maximumRecordSize int
	maxAggRecordSize  int
	stringGen         *util.RandomStringGenerator
}

// Config is for aggregation customizations.
type Config struct {
	MaximumRecordSize *int
	MaxAggRecordSize  *int
}

// NewAggregator create a new aggregator
func NewAggregator(stringGen *util.RandomStringGenerator, cfg *Config) *Aggregator {
	a := &Aggregator{
		partitionKeys:     make(map[string]uint64, 0),
		records:           make([]*Record, 0),
		maximumRecordSize: defaultMaximumRecordSize,
		maxAggRecordSize:  defaultMaxAggRecordSize,
		aggSize:           initialAggRecordSize,
		stringGen:         stringGen,
	}

	if cfg.MaximumRecordSize != nil {
		a.maximumRecordSize = *cfg.MaximumRecordSize
	}
	if cfg.MaxAggRecordSize != nil {
		a.maxAggRecordSize = *cfg.MaxAggRecordSize
	}

	return a
}

// AddRecord to the aggregate buffer.
// Will return a kinesis PutRecordsRequest once buffer is full, or if the data exceeds the aggregate limit.
func (a *Aggregator) AddRecord(partitionKey string, hasPartitionKey bool, data []byte) (entry *kinesis.PutRecordsRequestEntry, err error) {

	if hasPartitionKey {
		partitionKeySize := len([]byte(partitionKey))
		if partitionKeySize < 1 {
			return nil, fmt.Errorf("Invalid partition key provided")
		}
	}

	dataSize := len(data)

	// If this is a very large record, then don't aggregate it.
	if dataSize >= a.maxAggRecordSize {
		if !hasPartitionKey {
			partitionKey = a.stringGen.RandomString()
		}
		return &kinesis.PutRecordsRequestEntry{
			Data:         data,
			PartitionKey: aws.String(partitionKey),
		}, nil
	}

	if !hasPartitionKey {
		if len(a.partitionKeys) > 0 {
			// Take any partition key from the map, as long as one exists
			for k, _ := range a.partitionKeys {
				partitionKey = k
				break
			}
		} else {
			partitionKey = a.stringGen.RandomString()
		}
	}

	// Check if we need to add a new partition key, and if we do how much space it will take
	pKeyIdx, pKeyAddedSize := a.checkPartitionKey(partitionKey)

	// data field size is proto size of data + data field number size
	// partition key field size is varint of index size + field number size
	dataFieldSize := protowire.SizeBytes(dataSize) + fieldNumberSize
	pkeyFieldSize := protowire.SizeVarint(pKeyIdx) + fieldNumberSize
	// Total size is byte size of data + pkey field + field number of parent proto

	if a.getSize()+protowire.SizeBytes(dataFieldSize+pkeyFieldSize)+fieldNumberSize+pKeyAddedSize >= a.maximumRecordSize {
		// Aggregate records, and return if error
		entry, err = a.AggregateRecords()
		if err != nil {
			return entry, err
		}

		if !hasPartitionKey {
			// choose a new partition key if needed now that we've aggregated the previous records
			partitionKey = a.stringGen.RandomString()
		}
		// Recompute field size, since it changed
		pKeyIdx, _ = a.checkPartitionKey(partitionKey)
		pkeyFieldSize = protowire.SizeVarint(pKeyIdx) + fieldNumberSize
	}

	// Add new record, and update aggSize
	partitionKeyIndex := a.addPartitionKey(partitionKey)

	a.records = append(a.records, &Record{
		Data:              data,
		PartitionKeyIndex: &partitionKeyIndex,
	})

	a.aggSize += protowire.SizeBytes(dataFieldSize+pkeyFieldSize) + fieldNumberSize

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

	compressedData, err := compress.Compress(protoBufData)
	if err != nil {
		logrus.Warnf("Failed to compress records: %v", err)
		// This should not result in dropping records/increasing retries.
		// Compressor will return original data if it fails to compress them.
	}

	md5Sum := md5.New()
	md5Sum.Write(compressedData)
	md5CheckSum := md5Sum.Sum(nil)

	kclData := append(kclMagicNumber, compressedData...)
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

	partitionKeyLen := len([]byte(partitionKey))
	a.aggSize += protowire.SizeBytes(partitionKeyLen) + fieldNumberSize
	return idx
}

func (a *Aggregator) checkPartitionKey(partitionKey string) (uint64, int) {
	if idx, ok := a.partitionKeys[partitionKey]; ok {
		return idx, 0
	}

	idx := uint64(len(a.partitionKeys))
	partitionKeyLen := len([]byte(partitionKey))
	return idx, protowire.SizeBytes(partitionKeyLen) + fieldNumberSize
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
	return kclMagicNumberLen + md5.Size + a.aggSize
}

func (a *Aggregator) clearBuffer() {
	a.partitionKeys = make(map[string]uint64, 0)
	a.records = make([]*Record, 0)
	a.aggSize = initialAggRecordSize
}
