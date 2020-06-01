// Copyright 2019-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package main

import (
	"C"
	"fmt"
	"strings"
	"time"
	"unsafe"

	"github.com/aws/amazon-kinesis-firehose-for-fluent-bit/plugins"
	"github.com/aws/amazon-kinesis-streams-for-fluent-bit/kinesis"
	kinesisAPI "github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/sirupsen/logrus"
)

const (
	// Kinesis API Limit https://docs.aws.amazon.com/sdk-for-go/api/service/kinesis/#Kinesis.PutRecords
	maximumRecordsPerPut = 500
)

const (
	retries              = 6
	concurrentRetryLimit = 4
)

var (
	pluginInstances []*kinesis.OutputPlugin
)

func addPluginInstance(ctx unsafe.Pointer) error {
	pluginID := len(pluginInstances)
	output.FLBPluginSetContext(ctx, pluginID)
	instance, err := newKinesisOutput(ctx, pluginID)
	if err != nil {
		return err
	}

	pluginInstances = append(pluginInstances, instance)
	return nil
}

func getPluginInstance(ctx unsafe.Pointer) *kinesis.OutputPlugin {
	pluginID := output.FLBPluginGetContext(ctx).(int)
	return pluginInstances[pluginID]
}

func newKinesisOutput(ctx unsafe.Pointer, pluginID int) (*kinesis.OutputPlugin, error) {
	stream := output.FLBPluginConfigKey(ctx, "stream")
	logrus.Infof("[kinesis %d] plugin parameter stream = '%s'", pluginID, stream)
	region := output.FLBPluginConfigKey(ctx, "region")
	logrus.Infof("[kinesis %d] plugin parameter region = '%s'", pluginID, region)
	dataKeys := output.FLBPluginConfigKey(ctx, "data_keys")
	logrus.Infof("[kinesis %d] plugin parameter data_keys = '%s'", pluginID, dataKeys)
	partitionKey := output.FLBPluginConfigKey(ctx, "partition_key")
	logrus.Infof("[kinesis %d] plugin parameter partition_key = '%s'", pluginID, partitionKey)
	roleARN := output.FLBPluginConfigKey(ctx, "role_arn")
	logrus.Infof("[kinesis %d] plugin parameter role_arn = '%s'", pluginID, roleARN)
	endpoint := output.FLBPluginConfigKey(ctx, "endpoint")
	logrus.Infof("[kinesis %d] plugin parameter endpoint = '%s'", pluginID, endpoint)
	appendNewline := output.FLBPluginConfigKey(ctx, "append_newline")
	logrus.Infof("[kinesis %d] plugin parameter append_newline = %s", pluginID, appendNewline)
	timeKey := output.FLBPluginConfigKey(ctx, "time_key")
	logrus.Infof("[firehose %d] plugin parameter time_key = '%s'\n", pluginID, timeKey)
	timeKeyFmt := output.FLBPluginConfigKey(ctx, "time_key_format")
	logrus.Infof("[firehose %d] plugin parameter time_key_format = '%s'\n", pluginID, timeKeyFmt)

	if stream == "" || region == "" {
		return nil, fmt.Errorf("[kinesis %d] stream and region are required configuration parameters", pluginID)
	}

	if partitionKey == "log" {
		return nil, fmt.Errorf("[kinesis %d] 'log' cannot be set as the partition key", pluginID)
	}

	if partitionKey == "" {
		logrus.Infof("[kinesis %d] no partition key provided. A random one will be generated.", pluginID)
	}

	appendNL := false
	if strings.ToLower(appendNewline) == "true" {
		appendNL = true
	}
	return kinesis.NewOutputPlugin(region, stream, dataKeys, partitionKey, roleARN, endpoint, timeKey, timeKeyFmt, appendNL, pluginID)
}

// The "export" comments have syntactic meaning
// This is how the compiler knows a function should be callable from the C code

//export FLBPluginRegister
func FLBPluginRegister(ctx unsafe.Pointer) int {
	return output.FLBPluginRegister(ctx, "kinesis", "Amazon Kinesis Data Streams Fluent Bit Plugin.")
}

//export FLBPluginInit
func FLBPluginInit(ctx unsafe.Pointer) int {
	plugins.SetupLogger()
	err := addPluginInstance(ctx)
	if err != nil {
		logrus.Errorf("[kinesis] Failed to initialize plugin: %v\n", err)
		return output.FLB_ERROR
	}
	return output.FLB_OK
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	kinesisOutput := getPluginInstance(ctx)

	curRetries := kinesisOutput.GetConcurrentRetries()
	if curRetries > concurrentRetryLimit {
		logrus.Infof("[kinesis] flush returning retry, too many concurrent retries (%d)\n", curRetries)
		return output.FLB_RETRY
	}

	events, count, retCode := unpackRecords(kinesisOutput, data, length)
	if retCode != output.FLB_OK {
		logrus.Errorf("[kinesis] failed to unpackRecords\n")
		return retCode
	}
	go flushWithRetries(kinesisOutput, tag, count, events, retries)
	return output.FLB_OK
}

func flushWithRetries(kinesisOutput *kinesis.OutputPlugin, tag *C.char, count int, records []*kinesisAPI.PutRecordsRequestEntry, retries int) {
	var retCode, tries int

	currentRetries := kinesisOutput.GetConcurrentRetries()

	for tries = 0; tries < retries; tries++ {
		if currentRetries > 0 {
			// Wait if other goroutines are retrying, as well as implement a progressive backoff
			time.Sleep(time.Duration((2^currentRetries)*100) * time.Millisecond)
		}

		logrus.Debugf("[kinesis] Sending (%p) (%d) records, currentRetries=(%d)", records, len(records), currentRetries)
		retCode = pluginConcurrentFlush(kinesisOutput, tag, count, &records)
		if retCode != output.FLB_RETRY {
			break
		}
		currentRetries = kinesisOutput.AddConcurrentRetries(1)
		logrus.Infof("[kinesis] Going to retry with (%p) (%d) records, currentRetries=(%d)", records, len(records), currentRetries)
	}
	if tries > 0 {
		kinesisOutput.AddConcurrentRetries(int64(-tries))
	}
	if retCode == output.FLB_ERROR {
		logrus.Errorf("[kinesis] Failed to flush (%d) records with error", len(records))
	}
	if retCode == output.FLB_RETRY {
		logrus.Errorf("[kinesis] Failed flush (%d) records after retries %d", len(records), retries)
	}
}

func unpackRecords(kinesisOutput *kinesis.OutputPlugin, data unsafe.Pointer, length C.int) ([]*kinesisAPI.PutRecordsRequestEntry, int, int) {
	var ret int
	var ts interface{}
	var timestamp time.Time
	var record map[interface{}]interface{}
	count := 0

	records := make([]*kinesisAPI.PutRecordsRequestEntry, 0, maximumRecordsPerPut)

	// Create Fluent Bit decoder
	dec := output.NewDecoder(data, int(length))

	for {
		//Extract Record
		ret, ts, record = output.GetRecord(dec)
		if ret != 0 {
			break
		}

		switch tts := ts.(type) {
		case output.FLBTime:
			timestamp = tts.Time
		case uint64:
			// when ts is of type uint64 it appears to
			// be the amount of seconds since unix epoch.
			timestamp = time.Unix(int64(tts), 0)
		default:
			timestamp = time.Now()
		}

		retCode := kinesisOutput.AddRecord(&records, record, &timestamp)
		if retCode != output.FLB_OK {
			return nil, 0, retCode
		}

		count++
	}

	return records, count, output.FLB_OK
}

func pluginConcurrentFlush(kinesisOutput *kinesis.OutputPlugin, tag *C.char, count int, records *[]*kinesisAPI.PutRecordsRequestEntry) int {
	fluentTag := C.GoString(tag)
	logrus.Debugf("[kinesis %d] Found logs with tag: %s\n", kinesisOutput.PluginID, fluentTag)
	retCode := kinesisOutput.Flush(records)
	if retCode != output.FLB_OK {
		return retCode
	}
	logrus.Debugf("[kinesis %d] Processed %d events with tag %s\n", kinesisOutput.PluginID, count, fluentTag)

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	// Before final exit, call Flush() for all the instances of the Output Plugin
	// for i := range pluginInstances {
	// 	pluginInstances[i].Flush(records)
	// }

	return output.FLB_OK
}

func main() {
}
