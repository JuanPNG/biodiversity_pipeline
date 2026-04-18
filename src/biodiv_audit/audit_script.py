import os
from dotenv import load_dotenv

from src.biodiv_audit.helpers_audit import (
    get_species_to_check_taxonomy,
    get_expected_occurrence_file_names_from_taxonomy
)

load_dotenv()

# --- Configuration from environment ---
PROJECT_ID = os.getenv("PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET")
TABLE = "bp_log_taxonomy"

BUCKET_NAME = os.getenv("BUCKET_NAME")



import dotenv
import os

from biodiv_pipelines.utils.helpers import sanitize_species_name

dotenv.load_dotenv()

BUCKET_NAME = os.getenv("BUCKET_NAME")

from biodiv_audit.helpers_audit import (
    get_expected_occurrence_filename_map_from_taxonomy,
    get_expected_occurrence_file_names_from_taxonomy,
    get_raw_occurrence_filenames_from_gcs,
    filenames_to_species,
    get_dead_species_from_gcs,
    species_to_occurrence_filenames,
    get_cleaned_occurrence_filenames_from_gcs,
    get_spatial_annotation_species_from_gcs,
    collect_missed_species,
    write_audit_species_jsonl,
)

# Audit of occurrence downloads
expected_file_to_species = get_expected_occurrence_filename_map_from_taxonomy(
    bucket_name=BUCKET_NAME,
    blob_name="biodiv-pipelines-dev/runs/window_start=2026-04-09/run_ts=20260409T140858/taxonomy/taxonomy_validated.jsonl",
)

expected_files = set(expected_file_to_species.keys())

actual_files = get_raw_occurrence_filenames_from_gcs(
    bucket_name=BUCKET_NAME,
    raw_prefix="biodiv-pipelines-dev/runs/window_start=2026-04-09/run_ts=20260409T140858/occurrences/raw/",
)

dead_species = get_dead_species_from_gcs(
    bucket_name=BUCKET_NAME,
    dead_records_blob="biodiv-pipelines-dev/runs/window_start=2026-04-09/run_ts=20260409T140858/occurrences/raw/dead_records.jsonl",
)

dead_files = species_to_occurrence_filenames(dead_species)

# Total specie with missing raw files
#This includes both dead letters and anything missed in the download for unknown reasons
missing_raw_files = expected_files - actual_files

# Species missing raw files not captured in dead_records.jsonl
no_captured_species = expected_files - actual_files - dead_files

counter = 0
for i in dead_files:
    counter += 1
    if i in expected_files:
        print(f"{counter} Dead file {i} is also in expected files")

missing_species = [
    expected_file_to_species[fname] for fname in sorted(missing_raw_files)
]

# Audit of cleaned occurrences

cleaned_files = get_cleaned_occurrence_filenames_from_gcs(
    bucket_name=BUCKET_NAME,
    cleaned_prefix="biodiv-pipelines-dev/runs/window_start=2026-04-09/run_ts=20260409T140858/occurrences/cleaned/",
)
missing_cleaned_files = actual_files - cleaned_files
missing_cleaned_species = filenames_to_species(missing_cleaned_files)

#Audit of spatial annotations

spatial_species = get_spatial_annotation_species_from_gcs(
    bucket_name=BUCKET_NAME,
    spatial_summary_blob=(
        "biodiv-pipelines-dev/runs/window_start=2026-04-09/"
        "run_ts=20260409T140858/spatial/spatial_annotations_summary.jsonl"
    ),
)

cleaned_species = set(filenames_to_species(cleaned_files))
missing_spatial_species = sorted(cleaned_species - spatial_species)

# Compiling missed species

missed_species = collect_missed_species(
    missing_raw_files=missing_raw_files,
    missing_cleaned_files=missing_cleaned_files,
    missing_spatial_species=missing_spatial_species,
)

rerun_download_species = {
    sanitize_species_name(species)
    for species in filenames_to_species(missing_raw_files)
    if species
}

rerun_cleaning_species = {
    sanitize_species_name(species)
    for species in filenames_to_species(missing_cleaned_files)
    if species
}

rerun_spatial_species = {
    sanitize_species_name(species)
    for species in missing_spatial_species
    if species
}

# Extracting species from taxonomy_validated.jsonl
count, out_path = write_audit_species_jsonl(
    bucket_name=BUCKET_NAME,
    taxonomy_blob=(
        "biodiv-pipelines-dev/runs/window_start=2026-04-09/"
        "run_ts=20260409T140858/taxonomy/taxonomy_validated.jsonl"
    ),
    missed_species=missed_species,
    output_path="./out/audit/audit_missed_species.jsonl",
)

print("missed species from taxonomy_validated:", count)
print("written to:", out_path)
print("")

count, out_path = write_audit_species_jsonl(
    bucket_name=BUCKET_NAME,
    taxonomy_blob=(
        "biodiv-pipelines-dev/runs/window_start=2026-04-09/"
        "run_ts=20260409T140858/taxonomy/taxonomy_validated.jsonl"
    ),
    missed_species=rerun_download_species,
    output_path="./out/audit/audit_rerun_download.jsonl",
)

print("missed species from raw occurrences:", count)
print("written to:", out_path)
print("")

count, out_path = write_audit_species_jsonl(
    bucket_name=BUCKET_NAME,
    taxonomy_blob=(
        "biodiv-pipelines-dev/runs/window_start=2026-04-09/"
        "run_ts=20260409T140858/taxonomy/taxonomy_validated.jsonl"
    ),
    missed_species=rerun_cleaning_species,
    output_path="./out/audit/audit_rerun_cleaning.jsonl",
)

print("missed species from cleaned occurrences:", count)
print("written to:", out_path)
print("")

count, out_path = write_audit_species_jsonl(
    bucket_name=BUCKET_NAME,
    taxonomy_blob=(
        "biodiv-pipelines-dev/runs/window_start=2026-04-09/"
        "run_ts=20260409T140858/taxonomy/taxonomy_validated.jsonl"
    ),
    missed_species=rerun_spatial_species,
    output_path="./out/audit/audit_rerun_spatial.jsonl",
)

print("missed species from spatial annotation:", count)
print("written to:", out_path)
print("")
print("expected:", len(expected_files))
print("actual raw files:", len(actual_files))
print("cleaned files:", len(cleaned_files))
# print("dead-letter species:", len(dead_species))
print("missing raw files:", len(missing_raw_files))
# print(sorted(missing_raw_files))
print("")
print("missing after excluding dead letters:", len(no_captured_species))
# print(sorted(no_captured_species))
print("")
print("missing cleaned files:", len(missing_cleaned_files))
# print(sorted(missing_cleaned_files))
print("")
# print("cleaned species:", len(cleaned_species))
# print("spatial species:", len(spatial_species))
print("missing spatial:", len(missing_spatial_species))
# print(missing_spatial_species)
print("")
print("total missed species:", len(missed_species))
# print(sorted(missed_species)[:20])
# print("")