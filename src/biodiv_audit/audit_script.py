from pathlib import Path
from dotenv import load_dotenv

from biodiv_pipelines.utils.helpers import sanitize_species_name

from biodiv_audit.helpers_audit import (
    get_expected_occurrence_filename_map_from_taxonomy,
    get_raw_occurrence_filenames_from_gcs,
    filenames_to_species,
    get_dead_species_from_gcs,
    species_to_occurrence_filenames,
    get_cleaned_occurrence_filenames_from_gcs,
    get_spatial_annotation_species_from_gcs,
    collect_missed_species,
    write_audit_species_jsonl,
    write_taxonomy_tocheck_audit_jsonls,
    write_audit_summary_jsonl,
    parse_args,
)

load_dotenv()


def main() -> None:
    args = parse_args()

    project_id = args.project_id
    bucket_name = args.bucket_name
    bucket_prefix = args.bucket_prefix
    window_start = args.window_start
    run_ts = args.run_ts
    output_dir = Path(args.output_dir)

    run_id = f"window_start={window_start}/run_ts={run_ts}"
    run_path = f"{bucket_prefix}{run_id}"

    # Audit of occurrence downloads
    expected_file_to_species = get_expected_occurrence_filename_map_from_taxonomy(
        bucket_name=bucket_name,
        blob_name=f"{run_path}/taxonomy/taxonomy_validated.jsonl",
    )

    expected_files = set(expected_file_to_species.keys())

    actual_files = get_raw_occurrence_filenames_from_gcs(
        bucket_name=bucket_name,
        raw_prefix=f"{run_path}/occurrences/raw/",
    )

    dead_species = get_dead_species_from_gcs(
        bucket_name=bucket_name,
        dead_records_blob=f"{run_path}/occurrences/raw/dead_records.jsonl",
    )

    dead_files = species_to_occurrence_filenames(dead_species)

    # Total species with missing raw files
    # This includes both dead letters and anything missed in the download for unknown reasons
    missing_raw_files = expected_files - actual_files

    # Species missing raw files not captured in dead_records.jsonl
    no_captured_species = expected_files - actual_files - dead_files

    missing_species = [
        expected_file_to_species[fname] for fname in sorted(missing_raw_files)
    ]
    _ = missing_species

    # Audit of cleaned occurrences
    cleaned_files = get_cleaned_occurrence_filenames_from_gcs(
        bucket_name=bucket_name,
        cleaned_prefix=f"{run_path}/occurrences/cleaned/",
    )
    missing_cleaned_files = actual_files - cleaned_files
    missing_cleaned_species = filenames_to_species(missing_cleaned_files)
    _ = missing_cleaned_species

    # Audit of spatial annotations
    spatial_species = get_spatial_annotation_species_from_gcs(
        bucket_name=bucket_name,
        spatial_summary_blob=f"{run_path}/spatial/spatial_annotations_summary.jsonl",
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
        bucket_name=bucket_name,
        taxonomy_blob=f"{run_path}/taxonomy/taxonomy_validated.jsonl",
        missed_species=missed_species,
        output_path=output_dir / "audit_missed_validated_species.jsonl",
    )

    print("missed species from taxonomy_validated:", count)
    print("written to:", out_path)
    print("")

    count, out_path = write_audit_species_jsonl(
        bucket_name=bucket_name,
        taxonomy_blob=f"{run_path}/taxonomy/taxonomy_validated.jsonl",
        missed_species=rerun_download_species,
        output_path=output_dir / "audit_rerun_download.jsonl",
    )

    print("missed species from raw occurrences:", count)
    print("written to:", out_path)
    print("")

    count, out_path = write_audit_species_jsonl(
        bucket_name=bucket_name,
        taxonomy_blob=f"{run_path}/taxonomy/taxonomy_validated.jsonl",
        missed_species=rerun_cleaning_species,
        output_path=output_dir / "audit_rerun_cleaning.jsonl",
    )

    print("missed species from cleaned occurrences:", count)
    print("written to:", out_path)
    print("")

    count, out_path = write_audit_species_jsonl(
        bucket_name=bucket_name,
        taxonomy_blob=f"{run_path}/taxonomy/taxonomy_validated.jsonl",
        missed_species=rerun_spatial_species,
        output_path=output_dir / "audit_rerun_spatial.jsonl",
    )

    # Classifications of taxonomy_tocheck issues
    n_rerun, rerun_path, n_issue, issue_path = write_taxonomy_tocheck_audit_jsonls(
        bucket_name=bucket_name,
        taxonomy_tocheck_blob=f"{run_path}/taxonomy/taxonomy_tocheck.jsonl",
        output_dir=output_dir,
    )

    # Audit summary
    summary = {
        "total_species_in_run": len(expected_files),
        "total_species_missed_in_pipeline": len(missed_species) + n_issue + n_rerun,
        "taxonomy_issues": n_issue,
        "taxonomy_ena_rerun": n_rerun,
        "rerun_occs_download": len(missing_raw_files),
        "rerun_occs_cleaning": len(missing_cleaned_files),
        "rerun_spatial_issues": len(missing_spatial_species),
    }

    summary, summary_path = write_audit_summary_jsonl(
        summary=summary,
        output_path=output_dir / "audit_summary.jsonl",
    )

    print("missed species from spatial annotation:", count)
    print("written to:", out_path)
    print("")
    print("expected:", len(expected_files))
    print("actual raw files:", len(actual_files))
    print("cleaned files:", len(cleaned_files))
    print("missing raw files:", len(missing_raw_files))
    print("")
    print("missing after excluding dead letters:", len(no_captured_species))
    print("")
    print("missing cleaned files:", len(missing_cleaned_files))
    print("")
    print("missing spatial:", len(missing_spatial_species))
    print("")
    print("total missed species:", len(missed_species))
    print("")
    print("rerun taxonomy validation:", n_rerun, rerun_path)
    print("taxonomic issue:", n_issue, issue_path)
    print("")
    print("summary:", summary)
    print("summary path:", summary_path)


if __name__ == "__main__":
    main()