#!/bin/bash
docker build -t btrdb/buildenv:armv7 .
docker push btrdb/buildenv:armv7
