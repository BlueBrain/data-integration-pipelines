import argparse
from src.arguments import define_arguments


def trace_command_line_args(with_bucket=False, with_curated=False, with_e_type=False, with_really_update=False):

    parser = argparse.ArgumentParser()
    parser = define_arguments(parser, with_bucket=with_bucket)

    if with_curated:
        parser.add_argument(
            "--curated",
            help="Only check curated traces (yes), non-curated traces (no), or all traces (both)",
            type=str, required=True, choices=["yes", "no", "both"]
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
