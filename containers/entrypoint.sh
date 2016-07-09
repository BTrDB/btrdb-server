#!/bin/bash



: ${BTRDB_HTTP_ENABLED:=true}
: ${BTRDB_HTTP_PORT:=9000}
: ${BTRDB_CAPNP_ENABLED:=true}
: ${BTRDB_CAPNP_PORT:=4410}
: ${BTRDB_BLOCK_CACHE:=500000}
: ${BTRDB_MONGO_COLLECTION:=btrdb}
: ${BTRDB_EARLY_TRIP:=16384}
: ${BTRDB_INTERVAL:=5000}

set -e

if [ -z "$BTRDB_MONGO_SERVER" ]
then
  echo "The environment variable BTRDB_MONGO_SERVER must be set"
  exit 1
fi

cat >btrdb.conf <<EOF
[storage]
provider=file
filepath=/srv/

[http]
enabled=${BTRDB_HTTP_ENABLED}
port=${BTRDB_HTTP_PORT}
address=0.0.0.0

[capnp]
enabled=${BTRDB_CAPNP_ENABLED}
port=${BTRDB_CAPNP_PORT}
address=0.0.0.0

[mongo]
server=${BTRDB_MONGO_SERVER}
collection=${BTRDB_MONGO_COLLECTION}

[cache]
blockcache=${BTRDB_BLOCK_CACHE}

radosreadcache=256
radoswritecache=256

[coalescence]
earlytrip=${BTRDB_EARLY_TRIP}
interval=${BTRDB_INTERVAL}
EOF

if [[ $1 = "makedb" ]]
then
  echo "making database"
  btrdbd -makedb
  exit 0
fi

if [[ $1 = "bash" || $1 = "shell" ]]
then
  set +ex
  bash -i
  exit 0
fi

btrdbd |& panicparse
