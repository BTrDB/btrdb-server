#!/bin/bash
docker build --no-cache -t btrdb/buildenv .
docker push btrdb/buildenv
docker tag btrdb/buildenv btrdb/buildenv:amd64
docker push btrdb/buildenv:amd64
