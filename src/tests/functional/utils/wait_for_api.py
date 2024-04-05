import http
import os
import time

import requests
from dotenv import load_dotenv

load_dotenv()

if __name__ == '__main__':
    url = f"http://{os.environ.get('FASTAPI_HOST')}:" \
          f"{os.environ.get('FASTAPI_PORT')}/api/v1/health/check"
    while True:
        response = requests.get(url)
        if response.status_code == http.HTTPStatus.OK:
            break
        time.sleep(1)
