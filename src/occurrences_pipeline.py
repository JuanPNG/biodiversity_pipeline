import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from utils.transforms import WriteSpeciesOccurrencesFn


def occurrences_pipeline(args, beam_args):
    options = PipelineOptions(beam_args)
    with beam.Pipeline(options=options) as p:
        records = (
            p
            | 'ReadValidatedFile' >> beam.io.ReadFromText(args.validated_input)
            | 'ParseValidatedJson' >> beam.Map(json.loads)
        )

        results = records | 'FetchAndWriteOccurrences' >> beam.ParDo(
            WriteSpeciesOccurrencesFn(
                output_dir=args.output_dir,
                max_records=args.limit
            )
        ).with_outputs('dead', main='success')

        success = results.success
        dead = results.dead

        summary = (
            success
            | 'SuccessToKeyValue' >> beam.Map(lambda r: ('written', 1))
            | 'CountSuccesses' >> beam.CombinePerKey(sum)
        )

        dead_summary = (
            dead
            | 'DeadToKeyValue' >> beam.Map(lambda r: ('failed', 1))
            | 'CountFailures' >> beam.CombinePerKey(sum)
        )

        merged_summary = ((summary, dead_summary)
            | 'FlattenSummary' >> beam.Flatten())

        merged_summary | 'WriteSummary' >> beam.io.WriteToText(
            file_path_prefix=args.output_dir + '/summary',
            file_name_suffix='.jsonl',
            num_shards=1,
            shard_name_template='')

        _ = (dead
             | 'FilterDeadIfNotEmpty' >> beam.Filter(lambda x: x is not None)
             | 'WriteDeadRecords' >> beam.io.WriteToText(
                 file_path_prefix=args.output_dir + '/dead_records',
                 file_name_suffix='.jsonl',
                 num_shards=1,
                 shard_name_template='')
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='GBIF Occurrence Pipeline')
    parser.add_argument('--validated_input', required=True)
    parser.add_argument('--output_dir', required=True)
    parser.add_argument('--limit', type=int, default=150)

    args, beam_args = parser.parse_known_args()
    occurrences_pipeline(args, beam_args)
