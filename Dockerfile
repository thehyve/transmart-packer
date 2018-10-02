FROM python:3.6

ENV PYTHONUNBUFFERED 1

COPY ./requirements.txt /requirements.txt
COPY ./entrypoint.sh /entrypoint.sh

RUN pip install -r /requirements.txt &&\
    groupadd -r tornado && useradd -r -g tornado tornado &&\
    sed -i 's/\r//' /entrypoint.sh &&\
    chmod +x /entrypoint.sh

COPY . /app
RUN python /app/setup.py develop &&\
    chown -R tornado /app
WORKDIR /app
USER tornado

ENTRYPOINT ["/entrypoint.sh"]

