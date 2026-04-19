import argparse
import ast
import os

import datetime
import json

from pathlib import Path, PurePosixPath
from typing import Set
from google.cloud import bigquery, storage

from biodiv_pipelines.utils.helpers import sanitize_species_name


def get_species_to_check_taxonomy(project: str, dataset: str, table: str) -> tuple[int, Path]:
    """
    Query taxonomy records with status='to_check', save them to a timestamped CSV,
    and return the number of rows plus the output path.
    """
    table_id = f"{project}.{dataset}.{table}"

    client = bigquery.Client(project=project)

    query = f"""
    SELECT DISTINCT
      tax_id,
      accession,
      species,
      gbif_usageKey,
      gbif_matchType,
      gbif_confidence,
      gbif_scientificName,
      gbif_status,
      gbif_rank,
      date_seen
    FROM `{table_id}`
    WHERE status = 'to_check'
      AND species IS NOT NULL
    ORDER BY species
    """

    df_taxonomy_tocheck = client.query(query).to_dataframe()

    audit_dir = Path("./out/audit")
    audit_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    path_to_audit = audit_dir / f"taxonomy_to_check_{timestamp}.csv"

    df_taxonomy_tocheck.to_csv(path_to_audit, index=False)

    n_species = len(df_taxonomy_tocheck)

    return n_species, path_to_audit


def get_expected_occurrence_filename_map_from_taxonomy(
    bucket_name: str,
    blob_name: str,
) -> dict[str, str]:
    """
    Read taxonomy_validated.jsonl from GCS and return a mapping of expected raw
    occurrence filenames to the original taxonomy species names.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    filename_to_species: dict[str, str] = {}

    with blob.open("r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            record = json.loads(line)
            species = record.get("scientificName")
            if not species:
                continue

            safe_name = sanitize_species_name(species)
            filename_to_species[f"occ_{safe_name}.jsonl"] = species

    return filename_to_species


def get_expected_occurrence_file_names_from_taxonomy(
    bucket_name: str,
    blob_name: str,
) -> Set[str]:
    """
    Read taxonomy_validated.jsonl from GCS and return the expected raw occurrence
    filenames using the same sanitization logic as the occurrences pipeline.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    expected_files = set()

    with blob.open("r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            record = json.loads(line)
            species = record.get("scientificName")
            if not species:
                continue

            safe_name = sanitize_species_name(species)
            expected_files.add(f"occ_{safe_name}.jsonl")

    return expected_files


def get_raw_occurrence_filenames_from_gcs(
    bucket_name: str,
    raw_prefix: str,
) -> Set[str]:
    """
    List actual raw occurrence filenames under a GCS prefix and return only the
    basename of files matching occ_*.jsonl.
    """
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=raw_prefix)

    actual_files = set()

    for blob in blobs:
        name = PurePosixPath(blob.name).name

        if name.startswith("occ_") and name.endswith(".jsonl"):
            actual_files.add(name)

    return actual_files


def get_dead_species_from_gcs(
    bucket_name: str,
    dead_records_blob: str,
) -> Set[str]:
    """
    Read dead_records.jsonl from GCS and return the set of species that failed
    in the occurrences pipeline.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(dead_records_blob)

    dead_species = set()

    if not blob.exists():
        return dead_species  # no failures in this run

    with blob.open("r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            record = json.loads(line)
            sp = record.get("species")
            if sp:
                dead_species.add(sp)

    return dead_species


def filenames_to_species(filenames: Set[str]) -> list[str]:
    return sorted(
        name.removeprefix("occ_").removesuffix(".jsonl").replace("_", " ")
        for name in filenames
    )

def species_to_occurrence_filenames(species_names: set[str]) -> set[str]:
    """
    Convert species names to occurrence filenames using the same naming logic
    as the occurrences pipeline.
    """
    return {
        f"occ_{sanitize_species_name(species)}.jsonl"
        for species in species_names
        if species
    }


def get_cleaned_occurrence_filenames_from_gcs(
    bucket_name: str,
    cleaned_prefix: str,
) -> Set[str]:
    """
    List actual cleaned occurrence filenames under a GCS prefix and return only
    the basename of files matching occ_*.jsonl.
    """
    client = storage.Client()
    blobs = client.list_blobs(bucket_name, prefix=cleaned_prefix)

    cleaned_files = set()

    for blob in blobs:
        name = PurePosixPath(blob.name).name

        if name.startswith("occ_") and name.endswith(".jsonl"):
            cleaned_files.add(name)

    return cleaned_files


def get_spatial_annotation_species_from_gcs(
    bucket_name: str,
    spatial_summary_blob: str,
) -> Set[str]:
    """
    Read spatial_annotations_summ.jsonl from GCS and return the set of species
    found in the spatial annotation output.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(spatial_summary_blob)

    spatial_species = set()

    if not blob.exists():
        return spatial_species

    with blob.open("r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                record = ast.literal_eval(line)

            species = record.get("species")
            if species:
                spatial_species.add(species)

    return spatial_species


def collect_missed_species(
    missing_raw_files: Set[str],
    missing_cleaned_files: Set[str],
    missing_spatial_species: Set[str],
) -> Set[str]:
    """
    Combine all missed species across the audit stages into a single set.

    Assumes:
    - missing_raw_files and missing_cleaned_files are occurrence filenames
      already using the pipeline's sanitized naming convention.
    - missing_spatial_species already contains species names in comparable form.
    """
    missed_species = set()

    missed_species.update(filenames_to_species(missing_raw_files))
    missed_species.update(filenames_to_species(missing_cleaned_files))
    missed_species.update(species for species in missing_spatial_species if species)

    return {
        sanitize_species_name(species)
        for species in missed_species
        if species
    }


def write_audit_species_jsonl(
    bucket_name: str,
    taxonomy_blob: str,
    missed_species: Set[str],
    output_path: str | Path,
) -> tuple[int, Path]:
    """
    Read taxonomy_validated.jsonl from GCS, keep only records whose sanitized
    species name is in `missed_species`, and write them to a local JSONL file.
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(taxonomy_blob)

    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    matched = 0

    with blob.open("r") as src, output_path.open("w", encoding="utf-8") as dst:
        for line in src:
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                record = ast.literal_eval(line)

            species = record.get("scientificName")
            if not species:
                continue

            if sanitize_species_name(species) in missed_species:
                dst.write(json.dumps(record, ensure_ascii=False) + "\n")
                matched += 1

    return matched, output_path


def write_taxonomy_tocheck_audit_jsonls(
    bucket_name: str,
    taxonomy_tocheck_blob: str,
    output_dir: str | Path = "./out/audit",
) -> tuple[int, Path, int, Path]:
    """
    Read taxonomy_tocheck.jsonl from GCS and split it into:
    - rerun_taxonomy_validation.jsonl for records with ena_error
    - taxonomic_issue.jsonl for records with gbif_matchType

    Returns:
        (n_rerun_validation, rerun_validation_path,
         n_taxonomic_issue, taxonomic_issue_path)
    """
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(taxonomy_tocheck_blob)

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    rerun_validation_path = output_dir / "audit_rerun_taxonomy_validation.jsonl"
    taxonomic_issue_path = output_dir / "audit_rerun_taxonomic_issue.jsonl"

    n_rerun_validation = 0
    n_taxonomic_issue = 0

    with blob.open("r") as src, \
        rerun_validation_path.open("w", encoding="utf-8") as rerun_dst, \
        taxonomic_issue_path.open("w", encoding="utf-8") as issue_dst:

        for line in src:
            line = line.strip()
            if not line:
                continue

            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                record = ast.literal_eval(line)

            if "ena_error" in record and record["ena_error"]:
                rerun_dst.write(json.dumps(record, ensure_ascii=False) + "\n")
                n_rerun_validation += 1
            elif "gbif_matchType" in record:
                issue_dst.write(json.dumps(record, ensure_ascii=False) + "\n")
                n_taxonomic_issue += 1

    return (
        n_rerun_validation,
        rerun_validation_path,
        n_taxonomic_issue,
        taxonomic_issue_path,
    )


def write_audit_summary_jsonl(
    summary: dict[str, int],
    output_path: str | Path,
) -> tuple[dict[str, int], Path]:
    """
    Write a one-line JSONL audit summary from precomputed counters.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with output_path.open("w", encoding="utf-8") as f:
        f.write(json.dumps(summary, ensure_ascii=False) + "\n")

    return summary, output_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Audit a biodiversity pipeline run and write audit artifacts."
    )

    parser.add_argument(
        "--project-id",
        default=os.getenv("PROJECT_ID"),
        help="GCP project id. Defaults to PROJECT_ID env var.",
    )
    parser.add_argument(
        "--bucket-name",
        default=os.getenv("BUCKET_NAME"),
        help="GCS bucket name. Defaults to BUCKET_NAME env var.",
    )
    parser.add_argument(
        "--bucket-prefix",
        default="biodiv-pipelines-dev/runs/",
        help="Run root prefix inside the bucket.",
    )
    parser.add_argument(
        "--window-start",
        required=True,
        help="Run window start date, e.g. 2026-04-09.",
    )
    parser.add_argument(
        "--run-ts",
        required=True,
        help="Run timestamp, e.g. 20260409T140858.",
    )
    parser.add_argument(
        "--output-dir",
        default="./out/audit",
        help="Local directory where audit artifacts will be written.",
    )

    return parser.parse_args()