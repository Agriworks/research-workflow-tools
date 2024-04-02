import json
from pathlib import Path
from typing import Dict, List
import numpy as np

import pandas as pd

from panda_patch.utils import generate_update_patch_file
from research_workflow_tools.utils import generate_timestamp


def cast_value(value):
    # Try to convert to boolean
    if str(value).lower() in ["true", "false"]:
        return str(value).lower() == "true"
    # Try to convert to number
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            # Return as string
            return str(value)


def check_entry_against_value_dictionary(
    input_dictionary: Dict, column_name: str, column_value: str
) -> bool:
    # Check if the column name is in the dictionary
    if column_name not in input_dictionary:
        print(f"Column name {column_name} not in the dictionary, unable to check entry")
        return True

    # Check if the column value is in the dictionary
    if cast_value(column_value) not in list(input_dictionary[column_name].values()):
        print(
            f"Column value {cast_value(column_value)} not in the dictionary for column {column_name}, please check your entry"
        )
        return False

    return True


def generate_other_entry_workbook(
    data_in_path: Path = Path("./data_v0006.tsv"),
    ignore_list_path: Path = Path("./other_ignore_list.txt"),
    output_path: Path = Path("./"),
):
    """Generates a workbook that can be used to generate the other entry suggestions

    Args:
        data_in_path (Path, optional): _description_. Defaults to Path("./data_v0006.tsv").
        ignore_list_path (Path, optional): _description_. Defaults to Path("./other_ignore_list.txt").
        output_path (Path, optional): _description_. Defaults to Path("./").
    """

    # Step 0 - Create an ignore list
    ignore_list = []

    # Read the ignore list and add it to the ignore list
    with open(ignore_list_path, "r") as file_ptr:
        ignore_list += [line.strip() for line in file_ptr.read().splitlines()]

    # Step 1
    # Load the data into a dataframe
    # Load based on the file extension
    if data_in_path.suffix == ".xlsx":
        df = pd.read_excel(data_in_path)
    elif data_in_path.suffix == ".csv":
        df = pd.read_csv(data_in_path)
    elif data_in_path.suffix == ".tsv":
        df = pd.read_csv(data_in_path, sep="\t")
    else:
        print("Unknown file extension")
        exit(1)

    # Step 2 : Cycle through each of the columns and see if its type is a string column.
    human_entry_column_names = []
    for col in df.columns:
        if (df[col].dtype == "object") and (col not in ignore_list):
            human_entry_column_names.append(col)

    # Step 3:  Store all the unique values for each of the columns in a dictionary
    human_entry_column_values = {}
    for col in human_entry_column_names:
        human_entry_column_values[col] = list(
            set(
                [
                    item.strip()
                    for item in df[col].unique().tolist()
                    if type(item) == str
                ]
            )
        )

    # TODO: Step 4: Generate the suggested values for each of the column values
    # This will need to be a fuzzy / AI mediated step and will be done in the future

    # Step 5: Generate an Excel Sheet with column names unique values, and the suggested values for each of the columns

    # Step 5.1: Create a multi-indexed dataframe
    header = [
        "column_name",
        "unique_value",
        "replacement_value",
        "suggested_value",
        "delete_value",
        "new_column_name",
        "new_column_value",
        "delete_column_value",
    ]

    # Step 5.2: Create a dataframe with the header
    human_entry_df = pd.DataFrame(columns=header)

    # Step 5.3: Add the data to the dataframe
    for col in human_entry_column_values:
        for val in human_entry_column_values[col]:
            # If val is nan, then skip
            if pd.isna(val):
                continue

            data_to_append = {}
            data_to_append["column_name"] = col
            data_to_append["unique_value"] = val
            data_to_append["replacement_value"] = None
            data_to_append["suggested_value"] = None
            data_to_append["delete_value"] = False
            data_to_append["new_column_name"] = None
            data_to_append["new_column_value"] = None
            data_to_append["delete_column_value"] = None

            human_entry_df.loc[len(human_entry_df)] = data_to_append

    # Step 5.4: Write the dataframe to an excel file
    human_entry_df.to_csv("human_entry_suggestions.tsv", sep="\t", index=False)


def process_other_entry_replacements(
    data_in_path: Path,
    human_suggestions_path: Path,
    json_dictionary_path: Path = Path("./lookup-table/JSON_fields.json"),
    output_path: Path = Path("./"),
) -> Path:
    """Processes the other entry replacements

    Args:
        data_in_path (Path): Path of the data file against which we do the comparison
        human_suggestions_path (Path): Path of the human entry suggestions file filled out by the user
        json_dictionary_path (Path, optional): The json dictionary that has all the valid values for the different columns. Defaults to Path("./lookup-table/JSON_fields.json").
        output_path (Path, optional): Output folder where the patch file needs to go. Defaults to Path("./").

    Returns:
        Path: Path of the patch file
    """
    # Read data based on the file extension
    if data_in_path.suffix == ".xlsx":
        data_frame = pd.read_excel(data_in_path)
    elif data_in_path.suffix == ".csv":
        data_frame = pd.read_csv(data_in_path, sep=",")
    elif data_in_path.suffix == ".tsv":
        data_frame = pd.read_csv(data_in_path, sep="\t")

    data_frame_backup = data_frame.copy()

    # Read the csv/tsv/excel file into a dataframe
    # Check file extension
    if human_suggestions_path.suffix == ".xlsx":
        human_entry_df = pd.read_excel(human_suggestions_path)
    elif human_suggestions_path.suffix == ".csv":
        human_entry_df = pd.read_csv(human_suggestions_path, sep=",", header=0)
    elif human_suggestions_path.suffix == ".tsv":
        human_entry_df = pd.read_csv(human_suggestions_path, sep="\t", header=0)
    else:
        print("Unknown file extension")
        exit(1)

    # Retrieve all the new_column_name columns (they start with new_column_name)
    new_column_name_columns = [
        column_name
        for column_name in human_entry_df.columns
        if column_name.startswith("new_column_name")
    ]

    # Retrieve all the new_column_value columns (they start with new_column_value)
    new_column_value_columns = [
        column_name
        for column_name in human_entry_df.columns
        if column_name.startswith("new_column_value")
    ]

    # Retrieve all the delete_column_value columns (they start with delete_column_value)
    delete_column_value_columns = [
        column_name
        for column_name in human_entry_df.columns
        if column_name.startswith("delete_column_value")
    ]

    # Step 0: Strip all the columns that need to be stripped and check if the values
    # match the json dictionary

    # Step 0.1: Strip the columns to avoid human etry error

    # Strip the updated_values column
    strip_human_entry_values(
        human_entry_df,
        new_column_name_columns,
        new_column_value_columns,
        delete_column_value_columns,
    )

    # Step 0.2: Check if the values match the json dictionary

    # Get the replacement_value column's unique values

    # Step 1: Cycle through each of the rows and see if the replacement value is not null

    # Print all the columns values that have a replacement value
    to_fix_columns = human_entry_df["column_name"].unique().tolist()
    print("Columns that have a replacement value:")
    print(to_fix_columns)

    # Step 1.2: Check if the replacement value is in the dictionary
    # check_values_against_dictionary(
    #     json_dictionary_path=json_dictionary_path,
    #     data_frame=data_frame,
    #     human_entry_df_not_null=human_entry_df_not_null,
    #     new_column_name_columns=new_column_name_columns,
    #     new_column_value_columns=new_column_value_columns,
    #     delete_column_value_columns=delete_column_value_columns,
    # )
    # Step 2: Prep all the dataframes

    # Step 2.1: Trim all the string columns in the columns that have a replacement value
    for col in to_fix_columns:
        # Check if the column is a string column
        if data_frame[col].dtype == "object":
            # Trim the column
            data_frame[col] = data_frame[col].str.strip()

    # Step 2.2: Add the new columns to the old and new data frame if it doesn't exist
    newly_generated_columns = []
    to_delete_columns = []

    for new_column_name_column in new_column_name_columns:
        # Get the newly generated columns
        triggered_update_columns = (
            human_entry_df[new_column_name_column].unique().tolist()
        )

        # Add the new columns to the data frames
        for col in triggered_update_columns:
            # Add to newly generated columns
            if pd.isna(col):
                continue

            newly_generated_columns.append(col)
            # Check if the column exists in the data frame
            if col not in data_frame.columns:
                print(f"Column name {col} not in data frame, creating it")

                # Add the column to the data frame
                data_frame[col] = None
                data_frame_backup[col] = None

    # Step 3: Go through the human entry dataframe and replace the values in the data frame
    edit_hhids = []
    for index, row in human_entry_df.iterrows():
        print(f"Processing row {index}:")
        # Get the column name
        column_name = row["column_name"]
        # Get the unique value
        unique_value = row["unique_value"]
        # Get the replacement value
        replacement_value = row["replacement_value"]
        # Get the flag to delete the value
        delete_value = row["delete_value"]

        # Get the locations where the unique value is present
        locations = data_frame[column_name] == unique_value
        print(f"Processing row {index}: Locations: {sum(locations.tolist())}")

        # Get the hhids that need to be edited
        # TODO: Get the location indices instead of the hhids (it'll ensure that the code can work
        # with generic data frames plus it seems like its currently sort of broken since its capturing)
        # # Get the indices that need to be edited
        # edit_indices = data_frame[locations].index.tolist()
        ## Replacce with this later

        edit_hhids += data_frame[locations]["hhid"].unique().tolist()

        # Added the new value to the new column (this the followup update)
        for new_column_name, new_column_value in zip(
            new_column_name_columns, new_column_value_columns
        ):
            # Get the new column name
            update_column_name = row[new_column_name]
            # Get the new column value
            insertion_value = row[new_column_value]

            if pd.isna(insertion_value) or pd.isna(update_column_name):
                continue

            # Replace the value in the data frame
            data_frame.loc[locations, update_column_name] = cast_value(insertion_value)

            print(
                f"Processing row {index}: Inserting {insertion_value} in {update_column_name} as an update for {unique_value} in {column_name}"
            )

        # Delete the value in the data frame (this the followup update)
        for delete_column_name in delete_column_value_columns:
            if pd.isna(row[delete_column_name]):
                continue

            # Add to the to_delete_columns list
            to_delete_columns.append(row[delete_column_name])

            # Delete the value in the data frame
            data_frame.loc[locations, row[delete_column_name]] = np.nan

            print(
                f"Processing row {index}: Deleting {unique_value} in {row[delete_column_name]}"
            )

        if delete_value is True:
            # Delete the value in the data frame
            old_values = data_frame.loc[locations, column_name]
            data_frame.loc[locations, column_name] = np.nan
            new_values = data_frame.loc[locations, column_name]
            print(
                f"Processing row {index}: Deleting value {unique_value} in {column_name}"
            )
            print(f"Old values: {old_values}")
            print(f"New values: {new_values}")

        elif pd.isna(replacement_value) is False:
            # Replace the value in the data frame (this the actual update)
            data_frame.loc[locations, column_name] = cast_value(replacement_value)

            print(
                f"Processing row {index}: Replacing {unique_value} with {replacement_value} in {column_name}"
            )

    # Step 4: Generate a patch file by comparing the data frame with the original data frame
    # Step 4.1: Get the subset of the data frame that has the columns that need to be fixed

    # Step 4.2: Generate the patch file
    # Get the newly generated columns by doing a set difference
    # triggered_update_columns = list(
    #     set(data_frame.columns.tolist()) - set(data_frame_backup.columns.tolist())
    # )

    # Remove repeats for hhids
    edit_hhids = list(set(edit_hhids))

    # Remove repeats for to_delete_columns
    to_delete_columns = list(set(to_delete_columns))
    print(f"HHIDS to edit: {len(edit_hhids)}")
    print(edit_hhids)

    data_frame = data_frame[
        list(
            set(
                ["hhid", "redcap_event_name"]
                + to_fix_columns
                + newly_generated_columns
                + to_delete_columns
            )
        )
    ]

    print("Columns, to generate patches on:")
    print(data_frame.columns.tolist())

    # Filter the data_frame for only the edit_hhids:
    data_frame = data_frame[data_frame["hhid"].isin(edit_hhids)]

    data_frame.to_csv("others_diff.tsv", sep="\t")

    # Filter the backup data frame by the hhids
    data_frame_backup = data_frame_backup[data_frame_backup["hhid"].isin(edit_hhids)]

    patch_file_path = generate_update_patch_file(
        old_df=data_frame_backup,
        new_df=data_frame,
        id_columns=["hhid", "redcap_event_name"],
        patch_comment=f"Other Entry Replacement for columns: {data_frame.columns.tolist()}",
        new_file_name=f"other_entry_replacement_{generate_timestamp()}",
        trace=True,
        outpath=output_path,
    )

    return patch_file_path


def extract_not_null_df(
    human_entry_df, new_column_name_columns, new_column_value_columns
):
    condition = pd.isna(human_entry_df["replacement_value"]) | pd.isna(
        human_entry_df["delete_value"]
    )

    for new_column_name, new_column_value in zip(
        new_column_name_columns, new_column_value_columns
    ):
        condition |= pd.isna(human_entry_df[new_column_name]) & pd.isna(
            human_entry_df[new_column_value]
        )

    condition |= pd.isna(human_entry_df["delete_column_value"])

    human_entry_df_not_null = human_entry_df[condition].copy()
    return human_entry_df_not_null


def strip_human_entry_values(
    human_entry_df,
    new_column_name_columns,
    new_column_value_columns,
    delete_column_value_columns,
):
    if human_entry_df["replacement_value"].dtype == "object":
        human_entry_df["replacement_value"] = human_entry_df[
            "replacement_value"
        ].str.strip()

    # Strip all the columns that start with new_column_name
    for column_name in new_column_name_columns:
        if human_entry_df[column_name].dtype == "object":
            human_entry_df[column_name] = human_entry_df[column_name].str.strip()

    # Strip all the columns that start with new_column_value
    for column_name in new_column_value_columns:
        if human_entry_df[column_name].dtype == "object":
            human_entry_df[column_name] = human_entry_df[column_name].str.strip()

    # Strip all the columns that start with delete_column_value
    for column_name in delete_column_value_columns:
        if human_entry_df[column_name].dtype == "object":
            human_entry_df[column_name] = human_entry_df[column_name].str.strip()


def check_values_against_dictionary(
    json_dictionary_path: Path,
    data_frame: pd.DataFrame,
    human_entry_df_not_null: pd.DataFrame,
    new_column_name_columns: List[str],
    new_column_value_columns: List[str],
    delete_column_value_columns: List[str],
):
    json_dictionary = {}

    # Read the json dictionary
    with open(json_dictionary_path, "r") as file_ptr:
        json_dictionary = json.load(file_ptr)

    success_flag = True

    for index, row in human_entry_df_not_null.iterrows():
        # Get the column name
        column_name = row["column_name"]
        # Get the replacement value
        replacement_value = row["replacement_value"]

        # Check if the replacement value is in the dictionary
        if pd.isna(replacement_value) is False or pd.isna(column_name) is False:
            check_replacements_success = check_entry_against_value_dictionary(
                input_dictionary=json_dictionary,
                column_name=column_name,
                column_value=replacement_value,
            )

            success_flag &= check_replacements_success

        # Now repeat the check for all the new columns
        for new_column_name, new_column_value in zip(
            new_column_name_columns, new_column_value_columns
        ):
            # Get the new column name
            new_column_name = row[new_column_name]
            # Get the new column value
            new_column_value = row[new_column_value]

            # Check if the new column value is in the dictionary
            check_new_column_flag = check_entry_against_value_dictionary(
                input_dictionary=json_dictionary,
                column_name=new_column_name,
                column_value=new_column_value,
            )

            success_flag &= check_new_column_flag

        # Now repeat the check for all the delete columns
        for delete_column_name in delete_column_value_columns:
            # Get the delete column value
            todo_column_name = row[delete_column_name]

            # if null skip
            if pd.isnull(todo_column_name):
                continue

            # Check if the delete column value is in the data frame
            check_delete_success = todo_column_name in data_frame.columns.tolist()

            if check_delete_success is False:
                print(
                    f"Column value in column: {todo_column_name}  not in the data frame"
                )

            success_flag &= check_delete_success

    if success_flag is False:
        print("Exiting, Please check the entries in the human entry file")
        exit(1)
