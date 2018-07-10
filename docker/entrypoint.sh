#!/usr/bin/env bash

set -e
export REDIS_URL=redis://redis

exec "$@"
