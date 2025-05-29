import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.fileio import MatchFiles, ReadMatches
from utils.helpers import merge_annotations
from utils.transforms import (
    GenerateUncertaintyAreaFn,
    AnnotateWithCHELSAFn,
    AnnotateWithBiogeoFn,
    ClimateSummaryFn,
    BiogeoSummaryNestedFn
)


def spatial_annotation_pipeline(args, beam_args):
    options = PipelineOptions(beam_args)

    with beam.Pipeline(options=options) as p:
        # Load cleaned occurrences and create area of uncertainty
        cleaned = (
            p
            | "MatchFiles" >> beam.io.fileio.MatchFiles(args.input_occs)
            | "ReadMatches" >> beam.io.fileio.ReadMatches()
            | "ReadLines" >> beam.FlatMap(lambda f: [json.loads(line) for line in f.read_utf8().splitlines()])
            | "GenerateWKTBuffer" >> beam.ParDo(GenerateUncertaintyAreaFn())
        )

        # Clip the climate raster with the area of uncertainty and extract climate values
        climate_annotated = (
            cleaned
            | "AnnotateClimate" >> beam.ParDo(
                AnnotateWithCHELSAFn(args.climate_dir, output_key="clim_CHELSA")
            )
        )

        # Intersect area of uncertainty and biogeo vector layer to extract a list of intersected areas.
        # TODO Use area intersection threshold of 33% to list the area as potentially occupied.
        biogeo_annotated = (
            cleaned
            | "AnnotateBiogeo" >> beam.ParDo(
                AnnotateWithBiogeoFn(
                    vector_path=args.biogeo_vector,
                    keep_fields={"realm": "REALM", "biome": "BIOME_NAME", "ecoregion": "ECO_NAME"},
                    output_key="biogeo_Ecoregion"
                )
            )
        )

        # Prepare climate and biogeo annotations to be merged using GBIF occurrenceID as key.
        climate_kv = climate_annotated | "ClimateKV" >> beam.Map(lambda r: (r["occurrenceID"], r))
        biogeo_kv = biogeo_annotated | "BiogeoKV" >> beam.Map(lambda r: (r["occurrenceID"], r))

        joined = (
            {"climate": climate_kv, "biogeo": biogeo_kv}
            | "JoinByOccurrenceID" >> beam.CoGroupByKey()
            | "MergeAnnotations" >> beam.Map(lambda kv: merge_annotations(kv[1]))
        )

        # Write spatial annotations file: annotations per geographic coordinate
        _ = (
            joined
            | "ToJSON" >> beam.Map(json.dumps)
            | "WriteAnnotated" >> beam.io.WriteToText(
                file_path_prefix=args.annotated_output,
                file_name_suffix=".jsonl",
                num_shards=1
            )
        )

        # Prepare annotations summary per accession
        accession_kv = joined | "KeyByAccession" >> beam.Map(lambda r: ((r["accession"]), r))

        climate_summary = (
                accession_kv
                | "GroupClimate" >> beam.GroupByKey()
                | "SummarizeClimate" >> beam.ParDo(ClimateSummaryFn())
        )

        biogeo_summary = (
                accession_kv
                | "GroupBiogeo" >> beam.GroupByKey()
                | "SummarizeBiogeo" >> beam.ParDo(BiogeoSummaryNestedFn())
        )

        # Integrating climate and biogeographic annotations per accession
        joined_summ = (
            {"climate": climate_summary, "biogeo": biogeo_summary}
            | "JoinSummariesByAccession" >> beam.CoGroupByKey()
            | "MergeSummaries" >> beam.Map(lambda kv: {  # Unpacking annotation values
                **kv[1].get("climate", [{}])[0],
                **kv[1].get("biogeo", [{}])[0]
            })
        )

        _ = (
            joined_summ
            | "WriteJoinedSumm" >> beam.io.WriteToText(
                file_path_prefix=args.summary_output,
                file_name_suffix=".jsonl",
                num_shards=1
            )
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spatial annotation pipeline")

    parser.add_argument("--input_occs", required=True, help="Glob for cleaned JSONL files (quoted)")
    parser.add_argument("--climate_dir", required=True, help="Path to CHELSA dataset")
    parser.add_argument("--biogeo_vector", required=True, help="Path to vector dataset (e.g., ecoregions)")
    parser.add_argument("--annotated_output", required=True, help="Output path for full annotated records")
    parser.add_argument("--summary_output", required=True, help="Output path for the joined spatial summary")

    args, beam_args = parser.parse_known_args()
    spatial_annotation_pipeline(args, beam_args)