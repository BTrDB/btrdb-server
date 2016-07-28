#!/bin/bash
set -ex

if [ -z "$VER" ]; then
    echo "Need to set VER"
    exit 1
fi
mkdir -p $GOPATH/src/github.com/SoftwareDefinedBuildings
cd $GOPATH/src/github.com/SoftwareDefinedBuildings
git clone https://github.com/SoftwareDefinedBuildings/btrdb
cd btrdb
git checkout v$VER
go get -d ./...
cd btrdbd
DAT=$(date "+%F-%T")
go build -v -ldflags "-X github.com/SoftwareDefinedBuildings/btrdb.VersionString=$VER -X github.com/SoftwareDefinedBuildings/btrdb.BuildDate=$DAT"
cp btrdbd /
