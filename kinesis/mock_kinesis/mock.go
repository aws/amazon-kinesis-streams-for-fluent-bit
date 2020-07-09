// Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may
// not use this file except in compliance with the License. A copy of the
// License is located at
//
//     http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.

// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/aws/amazon-kinesis-streams-for-fluent-bit/kinesis (interfaces: PutRecordsClient)

// Package mock_kinesis is a generated GoMock package.
package mock_kinesis

import (
	reflect "reflect"

	kinesis "github.com/aws/aws-sdk-go/service/kinesis"
	gomock "github.com/golang/mock/gomock"
)

// MockPutRecordsClient is a mock of PutRecordsClient interface
type MockPutRecordsClient struct {
	ctrl     *gomock.Controller
	recorder *MockPutRecordsClientMockRecorder
}

// MockPutRecordsClientMockRecorder is the mock recorder for MockPutRecordsClient
type MockPutRecordsClientMockRecorder struct {
	mock *MockPutRecordsClient
}

// NewMockPutRecordsClient creates a new mock instance
func NewMockPutRecordsClient(ctrl *gomock.Controller) *MockPutRecordsClient {
	mock := &MockPutRecordsClient{ctrl: ctrl}
	mock.recorder = &MockPutRecordsClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockPutRecordsClient) EXPECT() *MockPutRecordsClientMockRecorder {
	return m.recorder
}

// PutRecords mocks base method
func (m *MockPutRecordsClient) PutRecords(arg0 *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PutRecords", arg0)
	ret0, _ := ret[0].(*kinesis.PutRecordsOutput)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// PutRecords indicates an expected call of PutRecords
func (mr *MockPutRecordsClientMockRecorder) PutRecords(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PutRecords", reflect.TypeOf((*MockPutRecordsClient)(nil).PutRecords), arg0)
}