"""
Dispatcher that routes to the appropriate pipeline wrapper
based on a `--pipeline` argument.
"""
import argparse
from flex_templates import taxonomy_flex # occurrences_flex, etc.


def launch_pipeline(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline", required=True, help="Name of the pipeline to run (e.g., taxonomy)")
    args, remaining_argv = parser.parse_known_args(argv)

    if args.pipeline == "taxonomy":
        taxonomy_flex.run(remaining_argv)
    else:
        raise ValueError(f"Unknown pipeline: {args.pipeline}")