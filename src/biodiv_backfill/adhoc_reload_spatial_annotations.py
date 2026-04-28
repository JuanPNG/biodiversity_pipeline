"""
Ad hoc: reload spatial annotation summary JSONL into BigQuery
Run as:
python -m biodiv_pipelines.load_spatial_annotation_summary_to_bq \
  --input "gs://<bucket>/out/spatial/summary*.jsonl" \
  --bq_summary_table "<project>.<dataset>.bp_spatial_annotations" \
  --bq_schema "gs://<bucket>/schemas/bq_spatial_annotation_summ_schema.json" \
  --temp_location "gs://<bucket>/temp" \
  --runner DataflowRunner \
  --project "<project>" \
  --region "<region>" \
  --staging_location "gs://<bucket>/staging" \
  --sdk_container_image "<image>" \
  --save_main_session

"""


import argparse
import ast
import json

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json


def load_spatial_annotation_summary_to_bq(args, beam_args):
    options = PipelineOptions(beam_args)

    bq_schema = None
    if args.bq_schema:
        with FileSystems.open(args.bq_schema) as f:
            schema_dict = json.load(f)
            schema_wrapped = json.dumps({"fields": schema_dict})
            bq_schema = parse_table_schema_from_json(schema_wrapped)

    with beam.Pipeline(options=options) as p:
        rows = (
            p
            | "ReadSummaryLines" >> beam.io.ReadFromText(args.input)
            | "ParsePythonDictLines" >> beam.Map(ast.literal_eval)
        )

        _ = (
            rows
            | "WriteSummaryToBigQuery" >> WriteToBigQuery(
                table=args.bq_summary_table,
                schema=bq_schema,
                method="FILE_LOADS",
                custom_gcs_temp_location=args.temp_location,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load persisted spatial annotation summary text into BigQuery"
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Input summary file(s), e.g. gs://bucket/out/spatial/summary*",
    )
    parser.add_argument(
        "--bq_summary_table",
        required=True,
        help="BigQuery table in format project.dataset.table",
    )
    parser.add_argument(
        "--bq_schema",
        required=True,
        help="Path to bq_spatial_annotation_summ_schema.json",
    )
    parser.add_argument(
        "--temp_location",
        required=True,
        help="GCS temp path for BigQuery file loads",
    )

    args, beam_args = parser.parse_known_args()
    load_spatial_annotation_summary_to_bq(args, beam_args)