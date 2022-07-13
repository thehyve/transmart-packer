FROM python:3.9-slim

ARG TRANSMART_PACKER_VERSION="0.7.1"

ENV PYTHONUNBUFFERED 1
ENV LOG_CFG docker-logging.yaml

EXPOSE 8999

COPY entrypoint.sh /entrypoint.sh
COPY docker-logging.yaml /docker-logging.yaml

RUN pip install --no-cache-dir --upgrade "transmart-packer == ${TRANSMART_PACKER_VERSION}" &&\
    groupadd -r tornado && useradd -r -g tornado tornado &&\
    sed -i 's/\r//' /entrypoint.sh &&\
    chmod +x /entrypoint.sh

RUN mkdir -p /app/tmp_data_dir
RUN chown -R tornado:tornado /app
WORKDIR /app
USER tornado

ENTRYPOINT ["/entrypoint.sh"]
