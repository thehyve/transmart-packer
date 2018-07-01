from redis import StrictRedis
from .config import redis_config

redis = StrictRedis(host=redis_config.get('host'), port=redis_config.get('port'), decode_responses=True)
