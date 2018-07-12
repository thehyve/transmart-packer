#!/usr/bin/env bash

set -e
export REDIS_URL=redis://redis
export DATA_DIR=/app/tmp_data_dir

exec "$@"
