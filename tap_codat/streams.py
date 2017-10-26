from singer import metrics
import pendulum
import time
from datetime import datetime, timedelta
from requests.exceptions import HTTPError
import json
import singer

LOGGER = singer.get_logger()


class Stream(object):
    """Information about and functions for syncing streams.

    Important class properties:

    :var tap_stream_id:
    :var pk_fields: A list of primary key fields"""
    def __init__(self, tap_stream_id, pk_fields, path):
        self.tap_stream_id = tap_stream_id
        self.pk_fields = pk_fields
        self.path = path

    def metrics(self, page):
        with metrics.record_counter(self.tap_stream_id) as counter:
            counter.increment(len(page))

    def format_response(self, response):
        return [response] if type(response) != list else response

    def write_page(self, page):
        """Formats a list of records in place and outputs the data to
        stdout."""
        singer.write_records(self.tap_stream_id, page)
        self.metrics(page)

    def transform(self, ctx, records):
        ret = []
        for record in records:
            ret.append(transform(record, ctx.schema_dicts[self.tap_stream_id]))
        return ret


class Companies(Stream):
    def sync(self, ctx):
        req = ctx.client.create_get_request(self.path)
        resp = ctx.client.request_with_handling(self.tap_stream_id, req)
        self.write_page(self.transform(ctx, resp["companies"]))

all_streams = [
    Companies("companies", ["id"], "/companies"),
]
all_stream_ids = [s.tap_stream_id for s in all_streams]
