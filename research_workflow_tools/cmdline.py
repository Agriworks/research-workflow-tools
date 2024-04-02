from pathlib import Path
import traceback

import click

from research_workflow_tools.other_entry_handler import (
    generate_other_entry_workbook,
    process_other_entry_replacements,
)



@click.command()
@click.argument(
    "data_in",
    nargs=1,
    required=True,
    type=click.Path(exists=True, path_type=Path),
)
@click.argument(
    "ignore_list",
    nargs=1,
    required=False,
    type=click.Path(exists=True, path_type=Path),
    default=Path("./other_ignore_list.txt"),
)
def process_human_entered_fields(data_in: Path, ignore_list: Path):
    generate_other_entry_workbook(data_in, ignore_list)


@click.command()
@click.argument(
    "data_in",
    nargs=1,
    required=True,
    type=click.Path(exists=True, path_type=Path),
)
@click.argument(
    "replacement_list",
    nargs=1,
    required=False,
    type=click.Path(exists=True, path_type=Path),
    default=Path("./human_entry_suggestions.xlsx"),
)
@click.argument(
    "input_dictionary",
    nargs=1,
    required=False,
    type=click.Path(exists=True, path_type=Path),
    default=Path("./lookup-table/JSON_fields.json"),
)
def process_human_suggesstions(
    data_in: Path, replacement_list: Path, input_dictionary: Path
):
    process_other_entry_replacements(data_in, replacement_list, input_dictionary)
