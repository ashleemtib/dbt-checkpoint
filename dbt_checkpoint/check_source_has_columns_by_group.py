import argparse
import os
import time
from itertools import groupby
from pathlib import Path
from typing import Any, Dict, Optional, Sequence

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    Column,
    add_default_args,
    get_dbt_manifest,
    get_parent_childs,
    get_source_schemas,
)
import json

def check_column_cnt(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    column_group: Dict[str, int],
    column_cnt: int,
) -> Dict[str, Any]:
    status_code = 0
    ymls = [Path(path) for path in paths]

    # if user added schema but did not rerun
    schemas = get_source_schemas(ymls)

    for schema in schemas:
        childs = list(
            get_parent_childs(
                manifest=manifest,
                obj=schema,
                manifest_node="nodes",
                node_types=["model"],
            )
        )

        columns = [column for column in childs if isinstance(column, Column)]

        grouped = groupby(
            sorted(columns, key=lambda x: x.column_name), lambda x: x.column_name
        )
        column_dict = {key: list(value) for key, value in grouped}
        required_column_count = 0
        for column in column_group:
            if column_dict.get(column):
                required_test_count += 1

        if required_column_count < column_cnt:
            print(
                f"{schema.source_name}.{schema.table_name}: "
                f"has only {required_test_count} column(s) from {column_group}.",
            )
            status_code = 1
    return {"status_code": status_code}

def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    parser.add_argument(
        "--columns",
        nargs="+",
        required=True,
        help="List of acceptable columns.",
    )
    parser.add_argument(
        "--column-cnt",
        type=int,
        default=1,
        help="Minimum number of columns required.",
    )

    args = parser.parse_args(argv)

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    hook_properties = check_column_cnt(
        paths=args.filenames,
        manifest=manifest,
        column_group=args.columns,
        column_cnt=args.column_cnt,
    )
    end_time = time.time()
    script_args = vars(args)

    tracker = dbtCheckpointTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check the source has a number of columns by group.",
            "status": hook_properties.get("status_code"),
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_test"),
        },
    )

    return hook_properties.get("status_code")


if __name__ == "__main__":
    exit(main())
