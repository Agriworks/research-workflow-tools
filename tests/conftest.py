import pandas as pd
import pytest


@pytest.fixture
def sample_patch_json():
    ret = {
        "patches": [
            {
                "target": {"hhid": "hhid-0001", "redcap_event_name": "default_event"},
                "deltas": {"col_A": "new_value_A", "col_B": "new_value_B"},
            }
        ],
        "version": 1,
        "meta": {
            "notes": "Give the comments and other notes necessary for this. Each of the objects in the patches will be a single change we need to have"
        },
    }

    return ret


def sample_data_frame():
    return pd.DataFrame()


def validate_data_frame_rows(df: pd.DataFrame):
    # Get all the columns that start with "expected_"
    expected_cols = [col for col in df.columns if col.startswith("expected_")]

    # Now, get the part after the prefix `expected_` save it as check_cols
    check_cols = [col.split("expected_")[1] for col in expected_cols]

    # Iterate over the rows of the dataframe
    for row_id, row in df.iterrows():
        # Iterate over the columns we want to check
        for col in check_cols:
            # Get the expected value
            expected_val = row[f"expected_{col}"]

            # Get the actual value
            actual_val = row[col]

            # First check if both are NaN using the pandas function
            if pd.isna(expected_val) and pd.isna(actual_val):
                assert True
                continue

            assert expected_val == actual_val, f"Assertion failed for row {row_id}"
