import os
import time

from elasticsearch import Elasticsearch

from settings import test_settings


if __name__ == '__main__':
    hosts = f"http://{test_settings.ELASTIC_HOST}:{test_settings.ELASTIC_PORT}"
    es_client = Elasticsearch(hosts=hosts)
    while True:
        if es_client.ping():
            break
        time.sleep(1)
