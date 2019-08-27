import os

tornado_config = dict(
    cookie_secret="__TODO:_GENERATE_YOUR_OWN_RANDOM_VALUE_HERE__",
    debug=False,
    port=9001
)

keycloak_config = dict(
    oidc_server_url="https://keycloak-dwh-test.thehyve.net/auth/realms/transmart-dev",
)

transmart_config = dict(
    host='https://transmart-dev.thehyve.net'
)

http_config = dict(
    verify_cert=True
)

redis_config = dict(
    url=os.environ.get('REDIS_URL', 'redis://localhost:6379'),
)

task_config = dict(
    data_dir='/tmp/packer/'
)

app_config = dict(
    host='https://glowingbear-dev.thehyve.net'
)
