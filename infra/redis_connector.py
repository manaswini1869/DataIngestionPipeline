import os
import logging
import redis
from redis.exceptions import ConnectionError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

REDIS_URL = os.getenv('REDIS_URI', 'redis://localhost:6379/0')

redis_pool = None

'''
Creates a redis connection pool
If exception occurs between connection loggs the error
Return the redis pool connection
'''
def get_redis_pool():
    global redis_pool
    if redis_pool is None:
        try:
            logging.info(f"Trying to create a Redis Connection pool for the URL: {REDIS_URL}")
            redis_pool = redis.ConnectionPool.from_url(REDIS_URL, decode_responses=True)
            temp_client = redis.Redis(connection_pool=redis_pool)
            temp_client.ping()
            logging.info("Redis connection created")
        except ConnectionError as e:
            logging.error(f"Failed to create a Redis connection pool: {e}")
            redis_pool = None
        except Exception as e:
            logging.error(f"Unexpected error while connecting to Redis: {e}")
    return redis_pool

'''
returns the redis connection
'''
def get_redis_connection():
    pool = get_redis_pool()
    if pool:
        try:
            r = redis.Redis(connection_pool=pool)
            r.ping()
            return r
        except ConnectionError as e:
            logging.error(f"Error getting redis connection: {e}")
            return None
    else:
        logging.error("Cannot get Redis connection, pool is not available")
        return None

'''
Sets the key-value pair in redis
Return True if the key and value added to redis else false
'''
def set_value(key: str, value: str, expire_seconds: int = None):
    r = get_redis_connection()
    if r:
        try:
            r.set(key, value, ex=expire_seconds)
            logging.info(f"Set redis key '{key}' and it expires in {expire_seconds}")
            return True
        except Exception as s:
            logging.error(f"Failed to set Redis key {key}")
    return False

'''
Gets the value for the key from redis cache
if key doesn't exist return None
'''
def get_value(key: str):
    r = get_redis_connection()
    value = None
    if r:
        try:
            value = r.get(key)
            logging.info(f"Got the value for the key: {key}")
        except Exception as e:
            logging.error(f"Got an error when getting key: {key}")
    return value

'''
Added the value to set if not already present
return the status of adding the value
Logs error if any
'''
def add_to_set(set_name: str, value: str):
    r = get_redis_connection()
    added = False
    if r:
        try:
            result = r.sadd(set_name, value)
            added = (result == 1)
            logging.info(f"Value '{value}' {'added to' if added else 'already exists in'} Redis set '{set_name}'")
        except Exception as e:
            logging.error(f"Failed to add the {value} because of exception: {e}")
    return added

'''
Checks foe the slug in the redis cache
'''
def is_member(set_name: str, value: str):
    r = get_redis_connection()
    member = False
    if r:
        try:
            member = r.sismember(set_name, value)
            logging.info(f"Checked membership for {value} in {set_name}: {member}")
        except Exception as e:
            logging.error(f"Received an error while checking the member if {value} in {set_name}: {e}")
    return member

if __name__ == "__main__":
    logging.info("Testing Redis Connection")
    redis_conn = get_redis_connection()
    if redis_conn:
        logging.info("Successfully obtained the redis connection")
    else:
        logging.error("Failed to connect to Redis")