import hashlib
import json

import psycopg
import psycopg_pool

from redis import Redis

__db_pool = psycopg_pool.ConnectionPool(
    conninfo="dbname=fedibgs user=postgres password=postgres host=10.10.10.12 port=5432",
    open=True,
)


__redis_url = "redis://10.10.10.12:6379"

__redis_connection = Redis(host="10.10.10.12", port=6379)

def get_db_connection():
    print("Getting connection")
    return __db_pool.connection()

def get_redis_connection():
    return __redis_connection

def get_redis_url():
    return __redis_url
