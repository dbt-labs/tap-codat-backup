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


class Basic(Stream):
    def sync(self, ctx):
        for company in ctx.cache["companies"]:
            path = self.path.format(companyId=company["id"])
            resp = ctx.client.GET({"path": path}, self.tap_stream_id)
            records = self.transform(ctx, self.format_response(resp, company))
            self.write_records(records)


class Financials(Stream):
    def sync(self, ctx):
        for company in ctx.cache["companies"]:
            path = self.path.format(companyId=company["id"])
            params = {
                "periodLength": ctx.config.get("financials_period_length", 1),
                "periodsToCompare": ctx.config.get("financials_periods_to_compare", 24),
            }
            resp = ctx.client.GET({"path": path, "params": params}, self.tap_stream_id)
            records = self.transform(ctx, self.format_response(resp, company))
            self.write_records(records)


def add_company_id(data, company):
    for record in data:
        record["companyId"] = company["id"]
    return data


def none_to_list(data, _):
    """Converts None to a list, otherwise returns data"""
    if data is None:
        return []
    return data


def dict_to_list(data, _):
    """Converts None to an empty list, otherwise wraps data in a list"""
    if data is None:
        return []
    return [data]


def key_getter(key):
    """Returns a function that will get the specified key from the dict if it
    exists and return an empty list otherwise."""
    return (lambda data, _: (data or {}).get(key, []))


def comp(f, g):
    return lambda data, company: f(g(data, company), company)


companies = Companies("companies", ["id"], "/companies")
all_streams = [
    companies,
    Basic("bank_statements", ["accountName"],
          "/companies/{companyId}/data/bankStatements",
          format_response=none_to_list),
    Basic("bills", ["id"], "/companies/{companyId}/data/bills",
          format_response=key_getter("bills")),
    Basic("company_info", ["companyId"], "/companies/{companyId}/data/info",
          format_response=comp(add_company_id, dict_to_list)),
    Basic("credit_notes", ["id"], "/companies/{companyId}/data/creditNotes",
          format_response=key_getter("creditNotes")),
    Basic("customers", ["id"], "/companies/{companyId}/data/customers",
          format_response=key_getter("customers")),
    Basic("invoices", ["id"], "/companies/{companyId}/data/invoices",
          format_response=key_getter("invoices")),
    Basic("payments", ["id"], "/companies/{companyId}/data/payments",
          format_response=key_getter("payments")),
    Basic("suppliers", ["id"], "/companies/{companyId}/data/suppliers",
          format_response=key_getter("suppliers")),
    Financials("balance_sheets", ["companyId"],
               "/companies/{companyId}/data/financials/balanceSheet",
               format_response=comp(add_company_id, dict_to_list)),
    Financials("profit_and_loss", ["companyId"],
               "/companies/{companyId}/data/financials/profitAndLoss",
               format_response=comp(add_company_id, dict_to_list)),
]
all_stream_ids = [s.tap_stream_id for s in all_streams]
