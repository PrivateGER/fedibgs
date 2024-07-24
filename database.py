import hashlib
import json

import psycopg

from redis import Redis
from clip_client import Client

__db_connection = psycopg.connect(
    dbname="fedibgs",
    user="postgres",
    password="postgres",
    host="10.10.10.12",
    port="5432",
    autocommit=True
)

__redis_url = "redis://10.10.10.12:6379"

__redis_connection = Redis(host="10.10.10.12", port=6379)

__clip_client = Client('http://localhost:51000')


def get_db_connection():
    return __db_connection


def get_cursor():
    return __db_connection.cursor()


def get_redis_connection():
    return __redis_connection

def get_redis_url():
    return __redis_url

def embed_or_cache(query, cache=True):
    redis = get_redis_connection()

    hash = hashlib.sha256(query.encode("utf8")).hexdigest()
    redis_key = f"query:{hash}"
    if redis.exists(redis_key) and cache:
        print(f"Cache hit for query: {query}")
        return json.loads(redis.get(redis_key))

    embedding = __clip_client.encode([query])[0].tolist()

    # json-serialize the embedding result and store it

    redis.set(redis_key, json.dumps(embedding))

    return embedding
