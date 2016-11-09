#!/bin/bash
set -ex

version=$(cat ../version.go | grep "VersionString" | cut -d' ' -f 4)
vmajor=$(cat ../version.go | grep "VersionMajor" | cut -d' ' -f 4)
vminor=$(cat ../version.go | grep "VersionMinor" | cut -d' ' -f 4)
vsubminor=$(cat ../version.go | grep "VersionSubminor" | cut -d' ' -f 4)

cp Dockerfile_ Dockerfile
echo "LABEL BTrDB_Version=$vmajor.$vminor.$vsubminor" >> Dockerfile

cp ../btrdbd/btrdbd .
cp `which panicparse` .

docker build --no-cache -t btrdb/release .
docker push btrdb/release

docker build -t btrdb/release:latest .
docker push btrdb/release:latest

docker build -t btrdb/release:$vmajor .
docker push btrdb/release:$vmajor

docker build -t btrdb/release:$vmajor.$vminor .
docker push btrdb/release:$vmajor.$vminor

docker build -t btrdb/release:$vmajor.$vminor.$vsubminor .
docker push btrdb/release:$vmajor.$vminor.$vsubminor
