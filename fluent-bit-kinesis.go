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

package main

import (
    "fmt"
    "C"
    "unsafe"
    "strings"

    "github.com/aws/amazon-kinesis-streams-for-fluent-bit/kinesis"
    "github.com/aws/amazon-kinesis-firehose-for-fluent-bit/plugins"
    "github.com/fluent/fluent-bit-go/output"
    "github.com/sirupsen/logrus"
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
    if  strings.ToLower(appendNewline) == "true" {
        appendNL = true
    }
    return kinesis.NewOutputPlugin(region, stream, dataKeys, partitionKey, roleARN, endpoint, appendNL, pluginID)
}


// The "export" comments have syntactic meaning
// This is how the compiler knows a function should be callable from the C code

//export FLBPluginRegister
func FLBPluginRegister(ctx unsafe.Pointer) int {
    return output.FLBPluginRegister(ctx, "kinesis", "Amazon Kinesis Data Streams Fluent Bit Plugin.")
}

//export FLBPluginInit
func  FLBPluginInit(ctx unsafe.Pointer) int {
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
    var count int
    var ret int
    var record map[interface{}]interface{}

    // Create Fluent Bit decoder
    dec := output.NewDecoder(data, int(length))

    kinesisOutput := getPluginInstance(ctx)
    fluentTag := C.GoString(tag)
    logrus.Debugf("[kinesis %d] Found logs with tag: %s\n", kinesisOutput.PluginID, fluentTag)

    for {
        //Extract Record
        ret, _, record = output.GetRecord(dec)
        if ret != 0 {
            break
        }

        retCode := kinesisOutput.AddRecord(record)
        if retCode != output.FLB_OK {
            return retCode
        }
        count++
    }
    err := kinesisOutput.Flush()
    if err != nil {
        logrus.Errorf("[kinesis %d] %v\n", kinesisOutput.PluginID, err)
        return output.FLB_ERROR
    }
    logrus.Debugf("[kinesis %d] Processed %d events with tag %s\n", kinesisOutput.PluginID, count, fluentTag)

    return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
    // Before final exit, call Flush() for all the instances of the Output Plugin
    for i := range pluginInstances {
        err := pluginInstances[i].Flush()
        if err != nil {
            logrus.Errorf("[kinesis %d] %v\n", pluginInstances[i].PluginID, err)
        }
    }

    return output.FLB_OK
}

func main() {
}
