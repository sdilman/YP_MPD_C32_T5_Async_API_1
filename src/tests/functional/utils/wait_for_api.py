import time
import os
import requests
import http

from dotenv import load_dotenv
from elasticsearch import Elasticsearch


load_dotenv()

if __name__ == '__main__':
    url = f"http://{os.environ.get('FASTAPI_HOST')}:{os.environ.get('FASTAPI_PORT')}/api/v1/health/check"
    while True:
        response = requests.get(url)
        if response.status_code == http.HTTPStatus.OK:
            break
        time.sleep(1)