

tornado_config = dict(
    cookie_secret="__TODO:_GENERATE_YOUR_OWN_RANDOM_VALUE_HERE__",
    debug=True,
    port=8999
)

keycloak_config = dict(
    oidc_server_url="https://keycloak-dwh-test.thehyve.net/auth/realms/transmart-dev",
)

redis_config = dict(
    host='localhost',
    port=6379
)
