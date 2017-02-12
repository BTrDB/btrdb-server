#!/bin/bash
set -ex

pushd ../btrdbd/
go build -v
popd
ver=$(../btrdbd/btrdbd -version)
cp ../btrdbd/btrdbd .
cp `which panicparse` .

docker build --no-cache  -t btrdb/k8s:${ver} .
docker push btrdb/k8s:${ver}
docker tag btrdb/k8s:${ver} btrdb/k8s:latest
docker push btrdb/k8s:latest
