import argparse
import os
import time
from itertools import groupby
from typing import Any, Dict, Optional, Sequence

from dbt_checkpoint.tracking import dbtCheckpointTracking
from dbt_checkpoint.utils import (
    JsonOpenError,
    ParseDict,
    Column,
    add_default_args,
    get_dbt_manifest,
    get_missing_file_paths,
    get_model_sqls,
    get_models,
    get_parent_childs,
)


def check_column_cnt(
    paths: Sequence[str],
    manifest: Dict[str, Any],
    required_cols: Dict[str, int],
    exclude_pattern: str,
) -> int:
    paths = get_missing_file_paths(
        paths, manifest, extensions=[".sql"], exclude_pattern=exclude_pattern
    )
    status_code = 0
    sqls = get_model_sqls(paths, manifest)
    filenames = set(sqls.keys())

    # get manifest nodes that pre-commit found as changed
    models = get_models(manifest, filenames)

    for model in models:
        childs = list(
            get_parent_childs(
                manifest=manifest,
                obj=model,
                manifest_node="nodes",
                node_types=["model"],
            )
        )
        columns = [column for column in childs if isinstance(column, Column)]
        grouped = groupby(
            sorted(columns, key=lambda x: x.column_name), lambda x: x.column_name
        )
        column_dict = {key: list(value) for key, value in grouped}
        for required_col, required_cnt in required_cols.items():
            col = column_dict.get(required_col, [])
            col_cnt = len(col)
            if not col or required_cnt > col_cnt:
                status_code = 1
                print(
                    f"{model.model_name}: "
                    f"has only {col_cnt} {required_col} columns, but "
                    f"{required_cnt} are required.",
                )
    return status_code


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser()
    add_default_args(parser)

    parser.add_argument(
        "--cols",
        metavar="KEY=VALUE",
        nargs="+",
        required=True,
        help="Set a number of key-value pairs."
        " Key is name of column and value is required "
        "minimal number of columns eg. --cols unique=1 not_null=2"
        "(do not put spaces before or after the = sign)."
        "",
        action=ParseDict,
    )

    args = parser.parse_args(argv)

    try:
        manifest = get_dbt_manifest(args)
    except JsonOpenError as e:
        print(f"Unable to load manifest file ({e})")
        return 1

    start_time = time.time()
    required_cols = {}
    for col_type, cnt in args.cols.items():
        try:
            col_cnt = int(cnt)
        except ValueError:
            parser.error(f"Unable to cast {cnt} to int.")
        required_cols[col_type] = col_cnt

    end_time = time.time()

    status_code = check_column_cnt(
        paths=args.filenames,
        manifest=manifest,
        required_tests=required_cols,
        exclude_pattern=args.exclude,
    )
    script_args = vars(args)

    tracker = dbtCheckpointTracking(script_args=script_args)
    tracker.track_hook_event(
        event_name="Hook Executed",
        manifest=manifest,
        event_properties={
            "hook_name": os.path.basename(__file__),
            "description": "Check model has columns by name",
            "status": status_code,
            "execution_time": end_time - start_time,
            "is_pytest": script_args.get("is_column"),
        },
    )

    return status_code


if __name__ == "__main__":
    exit(main())
