import polars as pl
from polars.testing import assert_series_equal


def test_map_keys_handles_dup_values():
    df = pl.DataFrame(
        {
            "original": [
                14,
                60,
                8,
                1048,
                30,
                832,
                142,
                20,
                42,
                174,
                38,
                824,
                0,
            ],
            "expected": [9, 11, 12, 5, 1, 13, 7, 4, 8, 10, 2, 6, 3],
        },
    )

    mapping = {
        824: 6,
        142: 7,
        1048: 5,
        832: 13,
        60: 11,
        14: 9,
        8: 12,
        20: 4,
        42: 8,
        174: 10,
        0: 3,
        30: 1,
        38: 2,
    }

    df = df.with_columns(
        pl.col("original")
        .map_elements(
            lambda x: mapping.get(x, x),
            pl.Int32,
        )
        .alias("mapped"),
    )

    assert_series_equal(
        df.get_column("mapped"),
        df.get_column("expected"),
        check_names=False,
    )
