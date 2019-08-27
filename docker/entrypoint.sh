#!/usr/bin/env bash

set -e
export REDIS_URL=redis://redis
export DATA_DIR=/app/tmp_data_dir

export VERIFY_CERT=true

CERTS_PATH="/ssl/certs.pem"
[[ -f "${CERTS_PATH}" ]] && \
  export VERIFY_CERT="${CERTS_PATH}"

exec "$@"
