#!/bin/bash
docker build --no-cache -t btrdb/buildenv:armv7 .
docker push btrdb/buildenv:armv7
