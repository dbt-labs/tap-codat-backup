from tap_codat.streams import flatten_report

sample_assets = \
{
    "name": "A",
    "value": 9.9,
    "items": [
        {
            "name": "A_A",
            "value": 2.2,
            "items": [
                {"name": "A_A_A", "value": 23},
                {"name": "A_A_B", "value": 32},
            ]
        },
        {"name": "A_B", "value": 100}
    ]
}

sample_flattened = [
    {"name": "A", "value": 9.9, "name_0": "A"},
    {"name": "A_A", "value": 2.2, "name_0": "A", "name_1": "A_A"},
    {"name": "A_A_A", "value": 23, "name_0": "A", "name_1": "A_A", "name_2": "A_A_A"},
    {"name": "A_A_B", "value": 32, "name_0": "A", "name_1": "A_A", "name_2": "A_A_B"},
    {"name": "A_B", "value": 100, "name_0": "A", "name_1": "A_B"},
]

def test_flatten():
    assert (
        sorted(flatten_report(sample_assets), key=(lambda x: x["name"]))
        ==
        sample_flattened
    )
