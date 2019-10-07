// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may
// not use this file except in compliance with the License. A copy of the
// License is located at
//
//  http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.

// Package kinesis containers the OutputPlugin which sends log records to Kinesis Stream
package kinesis

import (
    "fmt"
    "os"
    "time"
    "math/rand"

    "github.com/aws/amazon-kinesis-firehose-for-fluent-bit/plugins"
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/awserr"
    "github.com/aws/aws-sdk-go/aws/credentials/stscreds"
    "github.com/aws/aws-sdk-go/aws/endpoints"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/kinesis"
    fluentbit "github.com/fluent/fluent-bit-go/output"
    jsoniter "github.com/json-iterator/go"
    "github.com/sirupsen/logrus"
)

const (
    partitionKeyCharset = "abcdefghijklmnopqrstuvwxyz" + "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

const (
    // Kinesis API Limit https://docs.aws.amazon.com/sdk-for-go/api/service/kinesis/#Kinesis.PutRecords
    maximumRecordsPerPut      = 500
    maximumPutRecordBatchSize = 5242880 // 5 MB
    maximumRecordSize         = 1048576// 1 MB
)

// PutRecordsClient contains the kinesis PutRecords method call
type PutRecordsClient interface{
    PutRecords(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error)
}

type random struct {
    seededRandom    *rand.Rand
    buffer          []byte
}

// OutputPlugin sends log records to kinesis
type OutputPlugin struct {
    region                          string
    stream                          string
    dataKeys                        string
    partitionKey                    string
    lastInvalidPartitionKeyIndex    int
    client                          PutRecordsClient
    records                         []*kinesis.PutRecordsRequestEntry
    dataLength                      int
    backoff                         *plugins.Backoff
    timer                           *plugins.Timeout
    PluginID                        int
    random                          *random
}

// NewOutputPlugin creates an OutputPlugin object
func NewOutputPlugin(region, stream, dataKeys, partitionKey, roleARN, endpoint string, pluginID int) (*OutputPlugin, error) {
    sess, err := session.NewSession(&aws.Config{
        Region: aws.String(region),
    })
    if err != nil {
        return nil, err
    }

    client := newPutRecordsClient(roleARN, sess, endpoint)

    records := make([]*kinesis.PutRecordsRequestEntry, 0, maximumRecordsPerPut)
    timer, err := plugins.NewTimeout(func (d time.Duration) {
        logrus.Errorf("[kinesis %d] timeout threshold reached: Failed to send logs for %s\n", pluginID, d.String())
        logrus.Errorf("[kinesis %d] Quitting Fluent Bit", pluginID)
        os.Exit(1)
    })

    if err != nil {
        return nil, err
    }

    seededRand := rand.New(rand.NewSource(time.Now().UnixNano()))
    random := &random{
        seededRandom:   seededRand,
        buffer:         b := make([]byte, 8),
    }

    return &OutputPlugin{
        region:                         region,
        stream:                         stream,
        client:                         client,
        records:                        records,
        dataKeys:                       dataKeys,
        partitionKey:                   partitionKey,
        lastInvalidPartitionKeyIndex:   -1,
        backoff:                        plugins.NewBackoff(),
        timer:                          timer,
        PluginID:                       pluginID,
        random:                         random,
    }, nil
}

// newPutRecordsClient creates the Kinesis client for calling the PutRecords method
func newPutRecordsClient(roleARN string, sess *session.Session, endpoint string) *kinesis.Kinesis {
    svcConfig := &aws.Config{}
    if endpoint != "" {
        defaultResolver := endpoints.DefaultResolver()
        cwCustomResolverFn := func(service, region string, optFns ...func(*endpoints.Options)) (endpoints.ResolvedEndpoint, error) {
            if service == "kinesis" {
                return  endpoints.ResolvedEndpoint{
                    URL: endpoint,
                }, nil
            }
            return defaultResolver.EndpointFor(service, region, optFns...)
        }
        svcConfig.EndpointResolver = endpoints.ResolverFunc(cwCustomResolverFn)
    }

    if roleARN != "" {
        creds := stscreds.NewCredentials(sess, roleARN)
        svcConfig.Credentials = creds
    }

    client := kinesis.New(sess, svcConfig)
    client.Handlers.Build.PushBackNamed(plugins.CustomUserAgentHandler())
    return client
}

// AddRecord accepts a record and adds it to the buffer, flushing the buffer if it is full
// the return value is one of: FLB_OK FLB_RETRY
// API Errors lead to an FLB_RETRY, and all other errors are logged, the record is discarded and FLB_OK is returned
func (outputPlugin *OutputPlugin) AddRecord(record map[interface{}]interface{}) int {
    data, err := outputPlugin.processRecord(record)
    if err != nil {
        logrus.Errorf("[kinesis %d] %v\n", outputPlugin.PluginID, err)
        // discard this single bad record instead and let the batch continue
        return fluentbit.FLB_OK
    }

    newDataSize := len(data)

    if len(outputPlugin.records) == maximumRecordsPerPut || (outputPlugin.dataLength+newDataSize) > maximumPutRecordBatchSize {
        err = outputPlugin.sendCurrentBatch()
        if err != nil {
            logrus.Errorf("[kinesis %d] %v\n", outputPlugin.PluginID, err)
            // send failures are retryable
            return fluentbit.FLB_RETRY
        }
    }

    partitionKey := outputPlugin.getPartitionKey(record)
    outputPlugin.records = append(outputPlugin.records, &kinesis.PutRecordsRequestEntry{
        Data: data,
        PartitionKey: aws.String(partitionKey),
    })
    outputPlugin.dataLength += newDataSize
    return fluentbit.FLB_OK
}

// Flush sends the current buffer of records
func (outputPlugin *OutputPlugin) Flush() error {
    return outputPlugin.sendCurrentBatch()
}

func (outputPlugin *OutputPlugin) processRecord(record map[interface{}]interface{}) ([]byte, error) {
    if outputPlugin.dataKeys != "" {
        record = plugins.DataKeys(outputPlugin.dataKeys, record)
    }

    var err error
    record, err = plugins.DecodeMap(record)
    if err != nil {
        logrus.Debugf("[kinesis %d] Failed to decode record: %v\n", outputPlugin.PluginID, record)
        return nil, err
    }

    var json = jsoniter.ConfigCompatibleWithStandardLibrary
    data, err := json.Marshal(record)
    if err != nil {
        logrus.Debugf("[kinesis %d] Failed to marshal record: %v\n", outputPlugin.PluginID, record)
        return nil, err
    }

    // append newline
    data = append(data, []byte("\n")...)

    if len(data) > maximumRecordSize {
        return nil, fmt.Errorf("Log record greater than max size allowed by Kinesis")
    }

    return data, nil
}

func (outputPlugin *OutputPlugin) sendCurrentBatch() error {
    if outputPlugin.lastInvalidPartitionKeyIndex >= 0 {
        logrus.Errorf("[kinesis %d] Invalid partition key. Failed to find partition_key %s in log record %s.", outputPlugin.PluginID, outputPlugin.partitionKey, outputPlugin.records[outputPlugin.lastInvalidPartitionKeyIndex].Data)
        outputPlugin.lastInvalidPartitionKeyIndex = -1
    }
    outputPlugin.backoff.Wait()
    outputPlugin.timer.Check()

    response, err := outputPlugin.client.PutRecords(&kinesis.PutRecordsInput{
        Records:    outputPlugin.records,
        StreamName: aws.String(outputPlugin.stream),
    })
    if err != nil {
        logrus.Errorf("[kinesis %d] PutRecords failed with %v\n", outputPlugin.PluginID, err)
        outputPlugin.timer.Start()
        if aerr, ok := err.(awserr.Error); ok {
            if aerr.Code() == kinesis.ErrCodeProvisionedThroughputExceededException {
                logrus.Warnf("[kinesis %d] Throughput limits for the stream may have been exceeded.", outputPlugin.PluginID)
                // Backoff and Retry
                outputPlugin.backoff.StartBackoff()
            }
        }
        return err
    }
    logrus.Debugf("[kinesis %d] Sent %d events to Kinesis\n", outputPlugin.PluginID, len(outputPlugin.records))

    return outputPlugin.processAPIResponse(response)
}

// processAPIResponse processes the successful and failed records
// it returns an error iff no records succeeded (i.e.) no progress has been made
func (outputPlugin *OutputPlugin) processAPIResponse(response *kinesis.PutRecordsOutput) error {
    if aws.Int64Value(response.FailedRecordCount) > 0 {
        // start timer if all records failed (no progress has been made)
        if aws.Int64Value(response.FailedRecordCount) == int64(len(outputPlugin.records)) {
            outputPlugin.timer.Start()
            return fmt.Errorf("PutRecords request returned with no records successfully recieved")
        }

        logrus.Errorf("[kinesis %d] %d records failed to be delivered\n", outputPlugin.PluginID, aws.Int64Value(response.FailedRecordCount))
        failedRecords := make([]*kinesis.PutRecordsRequestEntry, 0, aws.Int64Value(response.FailedRecordCount))
        // try to resend failed records
        for i, record := range response.Records {
            if record.ErrorMessage != nil {
                logrus.Debugf("[kinesis %d] Record failed to send with error: %s\n", outputPlugin.PluginID, aws.StringValue(record.ErrorMessage))
                failedRecords = append(failedRecords, outputPlugin.records[i])
            }
            if aws.StringValue(record.ErrorCode) == kinesis.ErrCodeProvisionedThroughputExceededException {
                // Backoff and Retry
                outputPlugin.backoff.StartBackoff()
            }
        }

        outputPlugin.records = outputPlugin.records[:0]
        outputPlugin.records = append(outputPlugin.records, failedRecords...)
        outputPlugin.dataLength = 0
        for _, record := range outputPlugin.records {
            outputPlugin.dataLength += len(record.Data)
        }
    } else {
        //request fully succeeded
        for i, record := range response.Records {
            logrus.Debugf("[kinesis %d] record- %d:Shard ID: %s", outputPlugin.PluginID, i, aws.StringValue(record.ShardId))
        }
        outputPlugin.timer.Reset()
        outputPlugin.backoff.Reset()
        outputPlugin.records = outputPlugin.records[:0]
        outputPlugin.dataLength = 0
    }
    return nil
}

//randomString generates a random string of length 8
//it uses the math/rand library
func (outputPlugin *OutputPlugin) randomString() string {
    for i := range outputPlugin.random.buffer {
        outputPlugin.random.buffer[i] = partitionKeyCharset[outputPlugin.random.seededRandom.Intn(len(partitionKeyCharset))]
    }
    return string(outputPlugin.random.buffer)
}

//getPartitionKey returns the value for a given valid key
//if the given key is emapty or invalid, it returns a random string
func (outputPlugin *OutputPlugin) getPartitionKey(record map[interface{}]interface{}) string {
    partitionKey := outputPlugin.partitionKey
    if partitionKey != "" {
        for k, v := range record {
            dataKey := stringOrByteArray(k)
            if(dataKey == partitionKey) {
                value := stringOrByteArray(v)
                if(value != "") {
                    if len(value) > 256 {
                        value = value[0:256]
                    }
                    return value
                }
            }
        }
        outputPlugin.lastInvalidPartitionKeyIndex = len(outputPlugin.records)
     }
    return outputPlugin.randomString()
}

//stringOrByteArray returns the string value if the input is a string or byte array otherwise an empty string 
func stringOrByteArray(v interface{}) string {
    switch t := v.(type) {
        case []byte:
            return string(t)
        case string:
            return t
        default:
            return ""
    }
}
