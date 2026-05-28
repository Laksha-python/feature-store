import redis
import json

r = redis.Redis(
    host="localhost",
    port=6379,
    decode_responses=True
)

def redis_set(key, value, ttl=300):  
    try:
        r.set(key, json.dumps(value), ex=ttl)
    except Exception as e:
        print("Redis write failed:", e)


def redis_get(key):
    try:
        val = r.get(key)
        return json.loads(val) if val else None
    except Exception as e:
        print("Redis read failed:", e)
        return None