"""Ad-hoc script to rerun taxonomy validation for ENA GBIF pipeline

python -m biodiv_backfill.adhoc_rerun_ENA_GBIF_taxonomy_validation \
  --rerun_file ./out/rerun/rerun_taxonomy_validation.jsonl \
  --output out/backfill/taxonomy/taxonomy \
  --bq_table "<project>.<dataset>.bp_taxonomy_validated" \
  --bq_schema "gs://<bucket>/biodiv-pipelines-dev/schemas/bq_taxonomy_schema.json" \
  --temp_location "gs://<bucket>/biodiv-pipelines-dev/temp"

"""


import argparse
import json
import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions

from biodiv_pipelines.utils.helpers import (
    convert_dict_to_table_schema,
    to_gate_row
)
from biodiv_pipelines.utils.transforms import ENATaxonomyFn, ValidateNamesFn


def get_taxonomy_fields(record):
    return {
        "accession": record["accession"],
        "species": record["species"],
        "tax_id": record["tax_id"],
    }


def taxonomy_pipeline(args, beam_args):
    options = PipelineOptions(beam_args)

    with beam.Pipeline(options=options) as p:

        # Fetch species records from ElasticSearch
        es_records = (
                p
                | "ReadSpecies" >> beam.io.ReadFromText(args.rerun_file)
                | "ParseJSON" >> beam.Map(json.loads)
                | "CollectTaxonomyFields" >> beam.Map(get_taxonomy_fields)
                # | "PrintDEBBUG" >> beam.Map(print)
        )

        # Enrich from ENA API (with retry + optional delay)
        enriched = (
            es_records
            | "ReshuffleBeforeENA" >> beam.Reshuffle()
            | "FetchENATaxonomy" >> beam.ParDo(
                ENATaxonomyFn(
                    sleep_seconds=args.sleep,
                    include_lineage=True
                )
            )
            | "ReshuffleAfterENA" >> beam.Reshuffle()
        )

        # Validate species names against GBIF
        validated_output = (
            enriched
            | "ValidateGBIF" >> beam.ParDo(ValidateNamesFn()).with_outputs(ValidateNamesFn.TO_CHECK, main=ValidateNamesFn.VALIDATED)
        )

        validated = validated_output.validated
        unmatched = validated_output.to_check

        # Adding logs to bp_log_taxonomy Gate table append (validated + to_check)
        if args.bq_gate_table and args.temp_location:
            gate_schema = (
                "tax_id:STRING,"
                "accession:STRING,"
                "species:STRING,"
                "gbif_usageKey:INTEGER,"
                "gbif_matchType:STRING,"
                "gbif_rank:STRING,"
                "gbif_scientificName:STRING,"
                "gbif_status:STRING,"
                "gbif_confidence:INTEGER,"
                "date_seen:TIMESTAMP,"
                "status:STRING"
            )

            validated_gate = validated | "ToGateValidated" >> beam.Map(to_gate_row, status="validated")
            unmatched_gate = unmatched | "ToGateToCheck" >> beam.Map(to_gate_row, status="to_check")

            gate_rows = (validated_gate, unmatched_gate) | "FlattenGateRows" >> beam.Flatten()

            (
                gate_rows
                | "WriteGateToBigQuery" >> beam.io.WriteToBigQuery(
                    table=args.bq_gate_table,
                    schema=gate_schema,
                    method="FILE_LOADS",
                    custom_gcs_temp_location=args.temp_location,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                )
            )


        # Write validated records
        # Persisted checked taxonomy. Input for the occurrence pipeline.
        (
            validated
            | "ToJSONValidated" >> beam.Map(json.dumps)
            | "WriteValidated" >> beam.io.WriteToText(
                file_path_prefix=args.output + "_validated",
                file_name_suffix=".jsonl",
                num_shards=1,
                shard_name_template=""
            )
        )

        # Write unmatched/fuzzy/synonym records
        (
            unmatched
            | "ToJSONUnmatched" >> beam.Map(json.dumps)
            | "WriteUnmatched" >> beam.io.WriteToText(
                file_path_prefix=args.output + "_tocheck",
                file_name_suffix=".jsonl",
                num_shards=1,
                shard_name_template=""
            )
        )

        # Export to BigQuery
        if args.bq_table and args.bq_schema and args.temp_location:
            with FileSystems.open(args.bq_schema) as f:
                schema_dict = json.load(f)
                table_schema = convert_dict_to_table_schema(schema_dict)

            (
                validated
                | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                    table=args.bq_table,
                    schema=table_schema,
                    method="FILE_LOADS",
                    custom_gcs_temp_location=args.temp_location,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                )
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill ENA/GBIF taxonomy validation")

    parser.add_argument("--rerun_file", type=str, help="File to rerun taxonomy validation from. Fields accession, species, tax_id must be present.")

    # ENA throttling
    parser.add_argument("--sleep", type=float, default=0.25, help="Delay (in seconds) between ENA requests")

    # Output file
    parser.add_argument("--output", required=True, help="Output path prefix (no extension)")

    # BigQuery options
    parser.add_argument("--bq_table", help="BigQuery table (project.dataset.table)")
    parser.add_argument("--temp_location", help="GCS temp path for BQ file loads", required=False)
    parser.add_argument("--bq_schema", help="Path to BQ schema JSON")
    parser.add_argument("--bq_gate_table", help="BigQuery table for gating (project.dataset.table)")
    parser.add_argument("--bq_temp_dataset", help="BigQuery dataset for temporal query's results produced in apache beam (dataset)")

    args, beam_args = parser.parse_known_args()
    taxonomy_pipeline(args, beam_args)
