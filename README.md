# tap-codat

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [Codat](https://www.codat.io/)
- Extracts the following resources:
  - Accounts (Undocumented at the time of this writing)
  - [Companies](https://docs.codat.io/dedfd4e4-c241-4cb0-8001-f19eaf32d723/?python#companies)
  - [Balance Sheets](https://docs.codat.io/dedfd4e4-c241-4cb0-8001-f19eaf32d723/?python#balance-sheet)
  - [Bank Statements](https://docs.codat.io/dedfd4e4-c241-4cb0-8001-f19eaf32d723/?python#bank-statements)
  - [Bills](https://docs.codat.io/dedfd4e4-c241-4cb0-8001-f19eaf32d723/?python#bills)
  - [Company Info](https://docs.codat.io/dedfd4e4-c241-4cb0-8001-f19eaf32d723/?python#company-info)
  - [Credit Notes](https://docs.codat.io/dedfd4e4-c241-4cb0-8001-f19eaf32d723/?python#credit-notes)
  - [Customers](https://docs.codat.io/dedfd4e4-c241-4cb0-8001-f19eaf32d723/?python#customers)
  - [Invoices](https://docs.codat.io/dedfd4e4-c241-4cb0-8001-f19eaf32d723/?python#invoices)
  - [Payments](https://docs.codat.io/dedfd4e4-c241-4cb0-8001-f19eaf32d723/?python#payments)
  - [Profit and Loss](https://docs.codat.io/dedfd4e4-c241-4cb0-8001-f19eaf32d723/?python#profit-and-loss)
  - [Suppliers](https://docs.codat.io/dedfd4e4-c241-4cb0-8001-f19eaf32d723/?python#suppliers)
- Outputs the schema for each resource

## Quick Start

1. Install

    pip install tap-codat

2. Get an API key

   Refer to the Codat documentation
   [here](https://docs.codat.io/dedfd4e4-c241-4cb0-8001-f19eaf32d723/?python#authentication).

3. Create the config file

   You must create a JSON configuration file that looks like this:

   ```json
   {
     "start_date": "2010-01-01",
     "api_key": "your-api-key",
     "uat_urls": false
   }
   ```

   The `start_date` is the date at which the tap will begin pulling data for
   streams that support this feature. Note that in the initial version of this
   tap, this date is unused, as all streams replicate all of the data from
   Codat during every run.

   Replace `your-api-key` with the API key you received from Codat. If this
   token is for the UAT environment, change `uat_urls` to `true`.

4. Run the Tap in Discovery Mode

    tap-codat -c config.json -d

   See the Singer docs on discovery mode
   [here](https://github.com/singer-io/getting-started/blob/master/BEST_PRACTICES.md#discover-mode-and-connection-checks).

5. Run the Tap in Sync Mode

    tap-codat -c config.json -p catalog-file.json

## Data Formatting

For a few endpoints, this tap reformats the structure of "reports" received
from Codat. An example is the `balance_sheets` stream, which returns a
structure like this:

```json
"reports": [
    {
        "assets": {
            "name": "Top-level Category",
            "value": 1,
            "items": [
                {"name": "Inner category A", "value": 2},
                {"name": "Inner category B", "value": 3}
            ]
        }
    }
]
```

Here, `assets` describes a hierarchical structure. It is recursive in that any
of the `items` can themselves contain an array of items. This is not a
structure that easily can fit into a flat, tabular structure. To alleviate
this, this tap restructures this data into this format:


"reports": [
    {
        "assets": [
            {
                "name": "Top-level Category",
                "value": 1,
                "name_0": "Top-level Category"
            },
            {
                "name": "Inner category A",
                "value": 2,
                "name_0": "Top-level Category",
                "name_1": "Inner category A"
            },
            {
                "name": "Inner category B",
                "value": 3,
                "name_0": "Top-level Category",
                "name_1": "Inner category B"
            },
        ]
    }
]
```

The structure is flattened into a single array of objects. The `"name"` and
`"value"` properties are left as-is for each item, but now each items contains
properties `"name_X"` where X represents the category hierarchy. That is, if
your category hierarchy is

```
A
- B
- - C
- D
E
- F
```

Then `name_0` will always be either `A` or `E`, `name_1` will always be `B`,
`D`, or `F` and `name_2` will only ever be `C`.

---

Copyright &copy; 2017 Fishtown Analytics
