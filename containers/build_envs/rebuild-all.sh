#!/bin/bash

if [ -z "$ARM_BUILD_MACHINE" ]; then
  echo "Please set the IP of your ARM_BUILD_MACHINE"
  exit 1
fi

pushd amd64
./mkcontainer.sh
popd
pushd i686
./mkcontainer.sh
popd
set -ex
pushd arm7
ssh -p ${ARM_BUILD_MACHINE_PORT:-22} $ARM_BUILD_MACHINE "rm -rf btrdb-build-env; mkdir btrdb-build-env"
rsync -PHav -e "ssh -p ${ARM_BUILD_MACHINE_PORT:-22}" *  $ARM_BUILD_MACHINE:~/btrdb-build-env/
ssh -p ${ARM_BUILD_MACHINE_PORT:-22} $ARM_BUILD_MACHINE "cd btrdb-build-env; ./mkcontainer.sh"
