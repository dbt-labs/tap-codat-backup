from base64 import b64encode
import requests
from singer import metrics
import backoff

BASE_URL = "https://api.codat.io"
UAT_URL = "https://api-uat.codat.io"

    # r = requests.get("https://api-uat.codat.io/companies",
    #                  headers={"Authorization": "Basic " +  k})


class RateLimitException(Exception):
    pass


def _join(a, b):
    return a.rstrip("/") + "/" + b.lstrip("/")


class Client(object):
    def __init__(self, config):
        self.user_agent = config.get("user_agent")
        self.session = requests.Session()
        self.b64key = b64encode(config["api_key"].encode()).decode("utf-8")
        self.base_url = UAT_URL if config.get("uat_urls") else BASE_URL

    def prepare_and_send(self, request):
        if self.user_agent:
            request.headers["User-Agent"] = self.user_agent
        request.headers["Authorization"] = "Basic " + self.b64key
        return self.session.send(request.prepare())

    def url(self, path):
        return _join(self.base_url, path)

    def create_get_request(self, path, **kwargs):
        return requests.Request(
            method="GET",
            url=self.url(path),
            **kwargs,
        )

    @backoff.on_exception(backoff.expo,
                          RateLimitException,
                          max_tries=10,
                          factor=2)
    def request_with_handling(self, tap_stream_id, request):
        with metrics.http_request_timer(tap_stream_id) as timer:
            response = self.prepare_and_send(request)
            timer.tags[metrics.Tag.http_status_code] = response.status_code
        # FIXME raise RateLimitException appropriately
        response.raise_for_status()
        return response.json()
