import singer
from singer import metrics
from singer.transform import transform as tform
from .transform import transform_dts

LOGGER = singer.get_logger()


class Stream(object):
    def __init__(self, tap_stream_id, pk_fields, path,
                 returns_collection=True,
                 collection_key=None,
                 custom_formatter=None):
        self.tap_stream_id = tap_stream_id
        self.pk_fields = pk_fields
        self.path = path
        self.returns_collection = returns_collection
        self.collection_key = collection_key
        self.custom_formatter = custom_formatter or (lambda x: x)

    def metrics(self, records):
        with metrics.record_counter(self.tap_stream_id) as counter:
            counter.increment(len(records))

    def write_records(self, records):
        singer.write_records(self.tap_stream_id, records)
        self.metrics(records)

    def format_response(self, response, company):
        if self.returns_collection:
            if self.collection_key:
                records = (response or {}).get(self.collection_key, [])
            else:
                records = response or []
        else:
            records = [] if not response else [response]
        for record in records:
            record["companyId"] = company["id"]
        return self.custom_formatter(records)

    def transform_dts(self, ctx, records):
        return transform_dts(records, ctx.schema_dt_paths[self.tap_stream_id])


class Companies(Stream):
    def raw_fetch(self, ctx):
        return ctx.client.GET({"path": self.path}, self.tap_stream_id)

    def fetch_into_cache(self, ctx):
        resp = self.raw_fetch(ctx)
        ctx.cache["companies"] = self.transform_dts(ctx, resp["companies"])

    def sync(self, ctx):
        self.write_records(ctx.cache["companies"])


class Basic(Stream):
    def sync(self, ctx):
        for company in ctx.cache["companies"]:
            path = self.path.format(companyId=company["id"])
            resp = ctx.client.GET({"path": path}, self.tap_stream_id)
            records = self.transform_dts(ctx, self.format_response(resp, company))
            self.write_records(records)

PAGE_SIZE = 500


class Paginated(Stream):
    def sync(self, ctx):
        for company in ctx.cache["companies"]:
            path = self.path.format(companyId=company["id"])
            page = 1
            while True:
                params = {"pageSize": PAGE_SIZE, "page": page}
                resp = ctx.client.GET({"path": path, "params": params}, self.tap_stream_id)
                records = self.transform_dts(ctx, self.format_response(resp, company))
                self.write_records(records)
                if len(records) < PAGE_SIZE:
                    break
                page += 1


class Financials(Stream):
    def sync(self, ctx):
        for company in ctx.cache["companies"]:
            path = self.path.format(companyId=company["id"])
            params = {
                "periodLength": ctx.config.get("financials_period_length", 1),
                "periodsToCompare": ctx.config.get("financials_periods_to_compare", 24),
            }
            resp = ctx.client.GET({"path": path, "params": params}, self.tap_stream_id)
            records = self.transform_dts(ctx, self.format_response(resp, company))
            self.write_records(records)


def flatten_report(item, parent_names=[]):
    item_tformed = {
        "name": item["name"],
        "value": item["value"],
        "accountId": item["accountId"],
    }
    for idx, parent_name in enumerate(parent_names):
        item_tformed["name_" + str(idx)] = parent_name
    item_tformed["name_" + str(len(parent_names))] = item["name"]
    results = [item_tformed]
    sub_parent_names = parent_names + [item["name"]]
    for sub_item in item.get("items", []):
        results += flatten_report(sub_item, sub_parent_names)
    return results


def _update(dict_, key, function):
    dict_[key] = function(dict_[key])


def flatten_balance_sheets(balance_sheets):
    for balance_sheet in balance_sheets:
        for report in balance_sheet["reports"]:
            for key in ["assets", "liabilities", "equity"]:
                _update(report, key, flatten_report)
    return balance_sheets


def flatten_profit_and_loss(pnls):
    for pnl in pnls:
        for report in pnl["reports"]:
            for key in ["otherExpenses", "expenses", "costOfSales",
                        "otherIncome", "income"]:
                _update(report, key, flatten_report)
    return pnls


companies = Companies("companies", ["id"], "/companies")
all_streams = [
    companies,
    Basic("accounts", ["id"],
          "/companies/{companyId}/data/accounts",
          collection_key="accounts"),
    Basic("bank_statements", ["accountName"],
          "/companies/{companyId}/data/bankStatements"),
    Basic("bills", ["id"], "/companies/{companyId}/data/bills",
          collection_key="bills"),
    Basic("company_info", ["companyId"], "/companies/{companyId}/data/info",
          returns_collection=False),
    Basic("credit_notes", ["id"], "/companies/{companyId}/data/creditNotes",
          collection_key="creditNotes"),
    Basic("customers", ["id"], "/companies/{companyId}/data/customers",
          collection_key="customers"),
    Paginated("invoices", ["id"], "/companies/{companyId}/data/invoices",
              collection_key="results"),
    Basic("payments", ["id"], "/companies/{companyId}/data/payments",
          collection_key="payments"),
    Basic("suppliers", ["id"], "/companies/{companyId}/data/suppliers",
          collection_key="suppliers"),
    Financials("balance_sheets", ["companyId"],
               "/companies/{companyId}/data/financials/balanceSheet",
               returns_collection=False,
               custom_formatter=flatten_balance_sheets),
    Financials("profit_and_loss", ["companyId"],
               "/companies/{companyId}/data/financials/profitAndLoss",
               returns_collection=False,
               custom_formatter=flatten_profit_and_loss),
]
all_stream_ids = [s.tap_stream_id for s in all_streams]
