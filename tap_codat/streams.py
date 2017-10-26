import json
import time
from datetime import datetime, timedelta
import pendulum
from requests.exceptions import HTTPError
import singer
from singer import metrics
from singer.transform import transform as tform

LOGGER = singer.get_logger()


class Stream(object):
    def __init__(self, tap_stream_id, pk_fields, path, format_fn=None):
        self.tap_stream_id = tap_stream_id
        self.pk_fields = pk_fields
        self.path = path
        self.format_fn = format_fn or (lambda x: x)

    def metrics(self, page):
        with metrics.record_counter(self.tap_stream_id) as counter:
            counter.increment(len(page))

    def write_page(self, page):
        """Formats a list of records in place and outputs the data to
        stdout."""
        singer.write_records(self.tap_stream_id, page)
        self.metrics(page)

    def transform(self, ctx, records):
        ret = []
        for record in records:
            ret.append(tform(record, ctx.schema_dicts[self.tap_stream_id]))
        return self.format_fn(ret)


class Companies(Stream):
    def fetch_into_cache(self, ctx):
        resp = ctx.client.GET({"path": self.path}, self.tap_stream_id)
        companies = self.transform(ctx, resp["companies"])
        ctx.cache["companies"] = companies

    def sync(self, ctx):
        self.write_page(ctx.cache["companies"])


class Child(Stream):
    def sync(self, ctx):
        for company in ctx.cache["companies"]:
            path = self.path.format(companyId=company["id"])
            resp = ctx.client.GET({"path": path}, self.tap_stream_id, _404=[])
            self.write_page(self.transform(ctx, resp))


class Bills(Stream):
    def sync(self, ctx):
        for company in ctx.cache["companies"]:
            path = self.path.format(companyId=company["id"])
            resp = ctx.client.GET({"path": path}, self.tap_stream_id, _404={})
            bills = resp.get("bills", [])
            self.write_page(self.transform(ctx, bills))


class CompanyInfo(Stream):
    def sync(self, ctx):
        for company in ctx.cache["companies"]:
            path = self.path.format(companyId=company["id"])
            info = ctx.client.GET({"path": path}, self.tap_stream_id, _404={})
            info["companyId"] = company["id"]
            self.write_page(self.transform(ctx, [info]))


companies = Companies("companies", ["id"], "/companies")
all_streams = [
    companies,
    Child("bank_statements", ["accountName"], "/companies/{companyId}/data/bankStatements"),
    Bills("bills", ["id"], "/companies/{companyId}/data/bills"),
    CompanyInfo("company_info", ["companyId"], "/companies/{companyId}/data/info")
]
all_stream_ids = [s.tap_stream_id for s in all_streams]
