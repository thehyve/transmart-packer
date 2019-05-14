FROM python:3.6

ENV PYTHONUNBUFFERED 1
ENV LOG_CFG docker-logging.yaml

COPY ../requirements.txt /requirements.txt
COPY entrypoint.sh /entrypoint.sh
COPY docker-logging.yaml /docker-logging.yaml

RUN pip install -r /requirements.txt &&\
    groupadd -r tornado && useradd -r -g tornado tornado &&\
    sed -i 's/\r//' /entrypoint.sh &&\
    chmod +x /entrypoint.sh

COPY ../packer /app/packer
RUN mkdir -p /app/tmp_data_dir
RUN chown -R tornado:tornado /app
WORKDIR /app
USER tornado

ENTRYPOINT ["/entrypoint.sh"]

