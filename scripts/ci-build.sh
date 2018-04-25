#!/bin/bash

set -e

if [ "$SMOKE_TEST" ]; then
  echo "Currently in a smoke test, no build necessary"
  exit
fi

cd parquet-arrow

/home/ubuntu/rms-one-deployment/scripts/ci-build
