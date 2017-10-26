import singer
from singer import metrics
from singer.transform import transform as tform

LOGGER = singer.get_logger()


class Stream(object):
    def __init__(self, tap_stream_id, pk_fields, path,
                 format_response=(lambda resp, _: resp)):
        self.tap_stream_id = tap_stream_id
        self.pk_fields = pk_fields
        self.path = path
        self.format_response = format_response

    def metrics(self, records):
        with metrics.record_counter(self.tap_stream_id) as counter:
            counter.increment(len(records))

    def write_records(self, records):
        singer.write_records(self.tap_stream_id, records)
        self.metrics(records)

    def transform(self, ctx, records):
        ret = []
        for record in records:
            ret.append(tform(record, ctx.schema_dicts[self.tap_stream_id]))
        return ret


class Companies(Stream):
    def raw_fetch(self, ctx):
        return ctx.client.GET({"path": self.path}, self.tap_stream_id)

    def fetch_into_cache(self, ctx):
        resp = self.raw_fetch(ctx)
        ctx.cache["companies"] = self.transform(ctx, resp["companies"])

    def sync(self, ctx):
        self.write_records(ctx.cache["companies"])


class Child(Stream):
    def sync(self, ctx):
        for company in ctx.cache["companies"]:
            path = self.path.format(companyId=company["id"])
            resp = ctx.client.GET({"path": path}, self.tap_stream_id)
            records = self.transform(ctx, self.format_response(resp, company))
            self.write_records(records)


def format_company_info(info, company):
    if info is None:
        return []
    info["companyId"] = company["id"]
    return [info]


companies = Companies("companies", ["id"], "/companies")
all_streams = [
    companies,
    Child("bank_statements", ["accountName"],
          "/companies/{companyId}/data/bankStatements",
          format_response=(lambda resp, _: resp or [])),
    Child("bills", ["id"], "/companies/{companyId}/data/bills",
          format_response=(lambda resp, _: (resp or {}).get("bills", []))),
    Child("company_info", ["companyId"], "/companies/{companyId}/data/info",
          format_response=format_company_info),
    Child("credit_notes", ["id"], "/companies/{companyId}/data/creditNotes",
          format_response=(lambda resp, _: (resp or {}).get("creditNotes", []))),
    Child("customers", ["id"], "/companies/{companyId}/data/customers",
          format_response=(lambda resp, _: (resp or {}).get("customers", []))),
    Child("invoices", ["id"], "/companies/{companyId}/data/invoices",
          format_response=(lambda resp, _: (resp or {}).get("invoices", []))),
    Child("payments", ["id"], "/companies/{companyId}/data/payments",
          format_response=(lambda resp, _: (resp or {}).get("payments", []))),
    Child("suppliers", ["id"], "/companies/{companyId}/data/suppliers",
          format_response=(lambda resp, _: (resp or {}).get("suppliers", []))),
]
all_stream_ids = [s.tap_stream_id for s in all_streams]
