# Aggregation

This module implements KPL record aggregation.

https://github.com/awslabs/amazon-kinesis-producer/blob/master/aggregation-format.md


## Generating the aggregate.pb.go file from aggregate.proto

### Install protoc

https://developers.google.com/protocol-buffers/docs/downloads


### Install protoc-gen-go

    go get google.golang.org/protobuf/cmd/protoc-gen-g
    go install google.golang.org/protobuf/cmd/protoc-gen-go


### Install protobuf go library

    go get github.com/golang/protobuf


## Generating the protobuf go code

    protoc -I=. --go_out=. aggregate.proto

