import os

tornado_config = dict(
    cookie_secret="__TODO:_GENERATE_YOUR_OWN_RANDOM_VALUE_HERE__",
    debug=True,
    port=8999
)

keycloak_config = dict(
    oidc_server_url=os.environ.get('KEYCLOAK_URL'),
)

transmart_config = dict(
    host=os.environ.get('TRANSMART_URL')
)

app_config = dict(
    host=os.environ.get('CLIENT_ORIGIN_URL', 'http://localhost:4200')
)

redis_config = dict(
    url=os.environ.get('REDIS_URL', 'redis://localhost:6379'),
)

task_config = dict(
    data_dir=os.environ.get('DATA_DIR', '/tmp/packer/')
)

celery_config = dict(
    task_serializer='json',
    accept_content=['json'],  # Ignore other content
    result_serializer='json',
    timezone='Europe/Amsterdam',
    enable_utc=True,
)

logging_config = dict(
    path=os.environ.get('LOG_CFG', 'packer/logging.yaml')
)