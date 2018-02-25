#!/bin/bash
set -ex

pushd ../btrdbd/
go build -v 
popd
ver=$(../btrdbd/btrdbd -version)
cp ../btrdbd/btrdbd .

docker build -t btrdb/dev-db:${ver} .
docker push btrdb/dev-db:${ver}
docker tag btrdb/dev-db:${ver} btrdb/dev-db:latest
docker push btrdb/dev-db:latest
