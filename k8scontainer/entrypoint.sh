#!/bin/bash

: ${ETCD_ENDPOINT:=http://etcd.sgs.svc.cluster.local:2379}
: ${BTRDB_ADVERTISE_HTTP:=http://${MY_POD_IP}:9000}
: ${BTRDB_ADVERTISE_GRPC:=http://${MY_POD_IP}:4410}
: ${ETCD_PREFIX:=btrdb}
: ${CEPH_HOT_POOL:=btrdb}
: ${CEPH_DATA_POOL:=btrdb}

set -x
ls /etc/ceph
set +x

set -e


echo "ETCD endpoint is $ETCD_ENDPOINT"
cat >btrdb.conf <<EOF
# This is the configuration file for BTrDB
# without this file, it will not start. It should be
# located either in the directory from which btrdbd is
# started, or in /etc/btrdb/btrdb.conf

# BTrDB can run either in standalone mode, backed by files
# or in cluster mode, backed by ceph and running on multiple
# servers. Standalone mode is intended only for trying things
# out, and has several disadvantages.
[cluster]
  enabled=true
  # the key prefix in etcd
  prefix=${ETCD_PREFIX}

  # you should specify this multiple times to specify all of your endpoints
  etcdendpoint=${ETCD_ENDPOINT}

# ========================= NOTE =====================================
# if cluster.enabled=true above, then all of the options below will only
# be read on the FIRST boot of the BTrDB node. They are then copied into
# etcd and from point on, must be tweaked using btrdbctl

[storage]
  # If cluster mode is disabled above, then the data will be stored in files in
  # this directory
  filepath=/not/relevant

  # If cluster mode is enabled, then data will be written to the following
  cephdatapool=${CEPH_DATA_POOL}
  # If you specify a different pool here, internal nodes will be written
  # to this pool instead. These are typically < 1% of the total data
  cephhotpool=${CEPH_HOT_POOL}

  cephconf=/etc/ceph/ceph.conf

[http]
  enabled=true
  listen=0.0.0.0:9000
  advertise=${BTRDB_ADVERTISE_HTTP}

[grpc]
  enabled=true
  listen=0.0.0.0:4410
  advertise=${BTRDB_ADVERTISE_GRPC}

[cache]
  # Configure the RADOS and block caches. If you have a choice, rather
  # spend memory on the block cache.

  # This is measured in blocks, which are at most ~16K
  # blockcache=4000000 #64 GB
  # blockcache=2000000 #32 GB
  # blockcache=1000000 #16 GB
  # blockcache=500000  #8 GB
  # blockcache=250000  #4 GB
  blockcache=62500   #1 GB

  radosreadcache=2048 #in MB
  radoswritecache=256  #in MB

[coalescence]
  maxpoints=16384 #readings
  interval=5000 #ms
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
