import http
import os
import time

import requests

from settings import test_settings


if __name__ == '__main__':
    url = f"http://{test_settings.FASTAPI_HOST}:" \
          f"{test_settings.FASTAPI_PORT}/api/v1/health/check"
    while True:
        response = requests.get(url)
        if response.status_code == http.HTTPStatus.OK:
            break
        time.sleep(1)
