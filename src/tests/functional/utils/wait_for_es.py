import os
import time

from dotenv import load_dotenv
from elasticsearch import Elasticsearch

load_dotenv()

if __name__ == '__main__':
    es_client = Elasticsearch(hosts=f"http://{os.environ.get('ELASTIC_HOST')}:"
                                    f"{os.environ.get('ELASTIC_PORT')}")
    while True:
        if es_client.ping():
            break
        time.sleep(1)
