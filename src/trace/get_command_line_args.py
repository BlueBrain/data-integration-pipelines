import argparse
from datetime import datetime

from src.helpers import Deployment


def trace_command_line_args(with_bucket=False, with_curated=False, with_e_type=False, with_really_update=False):

    parser = argparse.ArgumentParser()
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")

    parser.add_argument(
        "--username", help="Service account username", type=str, required=True
    )
    parser.add_argument(
        "--password", help="Service account password", type=str, required=True
    )
    parser.add_argument(
        "--output_dir", help="Where to write the output of the task",
        default=f'./output/{timestamp}', type=str
    )

    parser.add_argument(
        "--deployment", type=str, default="PRODUCTION",
        choices=Deployment._member_names_
    )

    if with_curated:
        parser.add_argument(
            "--curated",
            help="Only check curated traces (yes), non-curated traces (no), or all traces (both)",
            type=str, required=True, choices=["yes", "no", "both"]
        )

    if with_bucket:
        parser.add_argument(
            "--bucket", help="Bucket to check", type=str, required=True
        )

    if with_e_type:
        parser.add_argument(
            "--e_type",
            help="Only check traces with e-type (yes), traces without e-type (no), or all traces (both)",
            type=str, default="no", choices=["yes", "no", "both"]
        )

    if with_really_update:
        parser.add_argument(
            "--really_update", help="Whether to really update data being modified by the pipeline in production",
            type=str, choices=["yes", "no"], required=True
        )

    return parser
