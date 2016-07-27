#!/bin/bash
docker build --no-cache -t btrdb/buildenv:i686 .
docker push btrdb/buildenv:i686
