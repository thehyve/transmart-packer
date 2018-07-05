import aioredis
from redis import StrictRedis

from .config import redis_config

redis = StrictRedis(host=redis_config.get('host'), port=redis_config.get('port'), decode_responses=True)


def get_async_redis(loop):
    return loop.run_until_complete(
        aioredis.create_redis_pool(
            (redis_config.get("host"), redis_config.get("port")),
            loop=loop,
            encoding='utf-8'
        )
    )