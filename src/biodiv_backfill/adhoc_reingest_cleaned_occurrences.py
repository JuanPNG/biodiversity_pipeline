import argparse
import json

import apache_beam as beam
from apache_beam.io import fileio, WriteToBigQuery, BigQueryDisposition
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions

from biodiv_pipelines.utils.helpers import convert_dict_to_table_schema

def normalise_occurrence_row(row: dict) -> dict:
    """Normalize historical field names to the current BigQuery schema."""
    if "license" not in row and "licence" in row:
        row["license"] = row.pop("licence")
    elif "licence" in row:
        row.pop("licence")

    return row


def reingest_cleaning_occurrences_pipeline(args, beam_args):
    """Read cleaned GBIF occurrence JSONL artifacts from GCS and load them into BigQuery."""
    options = PipelineOptions(beam_args)

    with FileSystems.open(args.bq_schema) as f:
        schema_dict = json.load(f)
    table_schema = convert_dict_to_table_schema(schema_dict)

    with beam.Pipeline(options=options) as p:
        rows = (
            p
            | "MatchFiles" >> fileio.MatchFiles(args.input_glob)
            | "ReadMatches" >> fileio.ReadMatches()
            | "ReadLines" >> beam.FlatMap(
                lambda file: file.read_utf8().splitlines()
            )
            | "ParseJsonRows" >> beam.Map(json.loads)
            | "NormalizeRows" >> beam.Map(normalise_occurrence_row)
        )

        _ = (
            rows
            | "WriteToBigQuery" >> WriteToBigQuery(
                table=args.bq_table,
                schema=table_schema,
                method="FILE_LOADS",
                custom_gcs_temp_location=args.temp_location,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ad hoc reingest of cleaned occurrence artifacts into BigQuery"
    )

    parser.add_argument(
        "--input_glob",
        required=True,
        help="GCS glob for cleaned JSONL files, e.g. gs://bucket/path/window_start=*/run_ts=*/occurrences/cleaned/occ_*.jsonl",
    )
    parser.add_argument(
        "--bq_table",
        required=True,
        help="BigQuery target table in the format project:dataset.table",
    )
    parser.add_argument(
        "--bq_schema",
        required=True,
        help="Path to BigQuery schema JSON, local path or gs:// path",
    )
    parser.add_argument(
        "--temp_location",
        required=True,
        help="GCS temp path for BigQuery load jobs",
    )

    args, beam_args = parser.parse_known_args()
    reingest_cleaning_occurrences_pipeline(args, beam_args)