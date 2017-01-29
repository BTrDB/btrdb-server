#!/bin/bash
set -ex

cp ../btrdbd/btrdbd .
cp `which panicparse` .

docker build  -t btrdb/k8s .
docker push btrdb/k8s
