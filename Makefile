# Copyright 2019-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
# 	http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

all: build

SOURCES := $(shell find . -name '*.go')
PLUGIN_BINARY := ./bin/kinesis.so
PLUGIN_VERSION := $(shell cat VERSION)

.PHONY: release
release:
	mkdir -p ./bin
	go build -buildmode c-shared -o ./bin/kinesis.so ./
	@echo "Built Amazon Kinesis Data Streams Fluent Bit Plugin v$(PLUGIN_VERSION)"

.PHONY: build
build: $(PLUGIN_BINARY) release plugin.tgz

$(PLUGIN_BINARY): $(SOURCES)
	PATH=${PATH} golint ./kinesis	

plugin.tgz: $(PLUGIN_BINARY)
	tar --strip-components 2 -zcvf plugin.tgz $(PLUGIN_BINARY)

.PHONY: generate
generate: $(SOURCES)
	go generate ./...

.PHONY: test
test:
	go test -timeout=120s -v -cover ./...

.PHONY: clean
clean:
	rm -rf ./bin/*
