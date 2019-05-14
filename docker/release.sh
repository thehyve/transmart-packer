#!/usr/bin/env bash

here=$(dirname "${0}")
TRANSMART_PACKER_VERSION=$(python "${here}/../print_version.py")

docker build --build-arg "TRANSMART_PACKER_VERSION=${TRANSMART_PACKER_VERSION}" -t "thehyve/transmart-packer:${TRANSMART_PACKER_VERSION}" "${here}" && \
docker login -u $DOCKER_USERNAME -p $DOCKER_PASSWORD && \
docker push "thehyve/transmart-packer:${TRANSMART_PACKER_VERSION}"
