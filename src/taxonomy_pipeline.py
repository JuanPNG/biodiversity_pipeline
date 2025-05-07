import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from utils.transforms import FetchESFn, ENATaxonomyFn, ValidateNamesFn


def taxonomy_pipeline(args, beam_args):
    options = PipelineOptions(beam_args)
    with beam.Pipeline(options=options) as p:
        es_records = (
            p
            | 'StartESFetch' >> beam.Create([None])
            | 'FetchFromES' >> beam.ParDo(FetchESFn(
                host=args.host,
                user=args.user,
                password=args.password,
                index=args.index,
                page_size=args.size,
                max_pages=args.pages
            ))
        )

        enriched = es_records | 'FetchENATaxonomy' >> beam.ParDo(ENATaxonomyFn(args.descendants))

        validated_outputs = enriched | 'ValidateGBIFNames' >> beam.ParDo(ValidateNamesFn()).with_outputs(
            ValidateNamesFn.TO_CHECK,
            main=ValidateNamesFn.VALIDATED
        )

        validated_outputs[ValidateNamesFn.VALIDATED] \
            | 'ToJsonValidated' >> beam.Map(json.dumps) \
            | 'WriteValidated' >> beam.io.WriteToText(
                args.output + '_validated', file_name_suffix='.jsonl', num_shards=1, shard_name_template='')

        validated_outputs[ValidateNamesFn.TO_CHECK] \
            | 'ToJsonToCheck' >> beam.Map(json.dumps) \
            | 'WriteToCheck' >> beam.io.WriteToText(
                args.output + '_tocheck', file_name_suffix='.jsonl', num_shards=1, shard_name_template='')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='GBIF Taxonomy Validation Pipeline')
    parser.add_argument('--host', required=True)
    parser.add_argument('--user', required=True)
    parser.add_argument('--password', required=True)
    parser.add_argument('--index', required=True)
    parser.add_argument('--size', type=int, default=1000)
    parser.add_argument('--pages', type=int, default=10)
    parser.add_argument('--descendants', action='store_true')
    parser.add_argument('--output', required=True)

    args, beam_args = parser.parse_known_args()
    taxonomy_pipeline(args, beam_args)
