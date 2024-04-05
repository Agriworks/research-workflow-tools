from pathlib import Path

import pandas as pd
from research_workflow_tools.other_entry_handler import (
    extract_not_null_df,
    process_other_entry_replacements,
)
from panda_patch.patchfile import PatchFile


def do_standard_comparision(data_in_path, human_suggestions_path, ref_patch_file_path):
    # Data set is called case_delete.tsv
    patch_file_path = process_other_entry_replacements(
        data_in_path=data_in_path,
        human_suggestions_path=human_suggestions_path,
        output_path=Path("tests/output"),
    )

    # Load the patch file and check that the correct entries are in there
    patch_file = PatchFile.parse_patch_file_from_path(patch_file_path)

    # Reference patchfile
    ref_patch_file = PatchFile.parse_patch_file_from_path(ref_patch_file_path)

    assert patch_file == ref_patch_file

    # Delete the patch file
    patch_file_path.unlink()


def test_process_other_entry_replacements():
    # Test the case where the user wants to delete the other entry
    # Dataset should have entries where other is only delete and no triggers

    do_standard_comparision(
        data_in_path=Path("tests/test_data/other_entry_dataset_case_delete.csv"),
        human_suggestions_path=Path(
            "tests/test_data/human_entry_suggestions_delete_case1.tsv"
        ),
        ref_patch_file_path=Path(
            "tests/test_data/other_entry_dataset_case_delete_case1.json"
        ),
    )


def test_process_other_entry_replacements_case_delete():
    # Dataset should have entries where other is delete and triggers for create
    do_standard_comparision(
        data_in_path=Path("tests/test_data/other_entry_dataset_case_delete.csv"),
        human_suggestions_path=Path(
            "tests/test_data/human_entry_suggestions_delete_case2.tsv"
        ),
        ref_patch_file_path=Path(
            "tests/test_data/other_entry_dataset_case_delete_case2.json"
        ),
    )


def test_process_other_entry_replacements_case_delete_plus_update():
    # Dataset should have entries where other is delete and triggers for update
    do_standard_comparision(
        data_in_path=Path(
            "tests/test_data/other_entry_dataset_case_delete_with_delete.csv"
        ),
        human_suggestions_path=Path(
            "tests/test_data/human_entry_suggestions_delete_case3.tsv"
        ),
        ref_patch_file_path=Path(
            "tests/test_data/other_entry_dataset_case_delete_case3.json"
        ),
    )


def test_process_other_entry_replacements_case_delete_plus_update_plus_create():
    # Dataset should have entries where other is delete and triggers for delete
    do_standard_comparision(
        data_in_path=Path(
            "tests/test_data/other_entry_dataset_case_delete_with_delete.csv"
        ),
        human_suggestions_path=Path(
            "tests/test_data/human_entry_suggestions_delete_case4.tsv"
        ),
        ref_patch_file_path=Path(
            "tests/test_data/other_entry_dataset_case_delete_case4.json"
        ),
    )


def test_process_other_entry_replacements_case_delete_plus_update_plus_create_plus_delete():
    # Dataset should have entries where other is delete and triggers for create and update and delete
    do_standard_comparision(
        data_in_path=Path(
            "tests/test_data/other_entry_dataset_case_delete_with_delete.csv"
        ),
        human_suggestions_path=Path(
            "tests/test_data/human_entry_suggestions_delete_case5.tsv"
        ),
        ref_patch_file_path=Path(
            "tests/test_data/other_entry_dataset_case_delete_case5.json"
        ),
    )


def test_extract_not_null_df():
    # TODO: Maybe get rid of this since the impact isn't that high
    # Case 1
    human_suggestions_df = pd.read_csv(
        Path("tests/test_data/human_entry_suggestions_delete_case1.tsv"), sep="\t"
    )

    not_null_ref_df = pd.read_csv(
        Path("tests/test_data/human_entry_suggestions_delete_case1_not_null.tsv"),
        sep="\t",
    )

    not_null_df = extract_not_null_df(
        human_entry_df=human_suggestions_df,
        new_column_name_columns=["new_column_name"],
        new_column_value_columns=["new_column_value"],
    )
    assert not_null_df.equals(not_null_ref_df)

    # Case 2

    # Case 3

    # Case 4

    # Case 5
