import time
import os

from dotenv import load_dotenv
from redis.asyncio import Redis


load_dotenv()

if __name__ == '__main__':
    redis_client = Redis(host={os.environ.get('REDIS_HOST')}, port={os.environ.get('REDIS_PORT')})
    while True:
        if redis_client.ping():
            break
        time.sleep(1)