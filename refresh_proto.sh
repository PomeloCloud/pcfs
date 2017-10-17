#!/usr/bin/env bash
protoc -I ./ ./proto/api.proto --go_out=plugins=grpc:./