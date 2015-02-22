#!/bin/bash -ex
rm -f profile.*
rm -f log.*
export goversion=go_64_1.4.2
export GOROOT=/srv/$goversion
export GO=$GOROOT/bin/go
mkdir -p gopath
export GOPATH=`pwd`/gopath
export PATH=$PATH:$GOROOT/bin/
git pull
$GO get -v -d ./...
$GO build -a -v -o exe ./qserver
export CEPHTYPE=filestore
export TEST_TYPE=loadgen2
ipython qci/runtests.ipy
