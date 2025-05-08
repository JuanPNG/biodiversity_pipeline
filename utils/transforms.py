import json
from elasticsearch import Elasticsearch
from apache_beam import DoFn, pvalue
from apache_beam.metrics import Metrics
from apache_beam.io.filesystems import FileSystems
from pygbif import species as gbif_spp, occurrences as gbif_occ
from utils.helpers import sanitize_species_name
import time

class FetchESFn(DoFn):
    def __init__(self, host, user, password, index, page_size, max_pages, *unused_args, **unused_kwargs):
        super().__init__(*unused_args, **unused_kwargs)
        self.host = host
        self.user = user
        self.password = password
        self.index = index
        self.page_size = page_size
        self.max_pages = max_pages

    def setup(self):
        self.es = Elasticsearch(
            hosts=self.host,
            basic_auth=(self.user, self.password)
        )

    def process(self, element):
        after = None
        for _ in range(self.max_pages):
            query = {
                'size': self.page_size,
                'sort': {'tax_id': 'asc'},
                'query': {'match': {'annotation_complete': 'Done'}}
            }
            if after:
                query['search_after'] = after

            response = self.es.search(index=self.index, body=query)
            hits = response.get('hits', {}).get('hits', [])
            if not hits:
                break

            for hit in hits:
                ann = hit['_source']['annotation'][-1]
                yield {
                    'accession': ann['accession'],
                    'species': ann['species'],
                    'tax_id': hit['_source']['tax_id']
                }

            after = hits[-1].get('sort')


class ENATaxonomyFn(DoFn):
    """
    Fetches taxonomy info from ENA by tax_id.
    Includes retry with backoff and optional sleep for throttling.
    """

    def __init__(self, include_lineage=True, sleep_seconds=0.25, max_retries=3):
        self.include_lineage = include_lineage
        self.sleep_seconds = sleep_seconds
        self.timeout = 10
        self.max_retries = max_retries

    def setup(self):
        import requests
        from lxml import etree
        self.requests = requests
        self.etree = etree

    def get_with_retries(self, url):
        for i in range(self.max_retries):
            try:
                resp = self.requests.get(url, timeout=self.timeout)
                resp.raise_for_status()
                return resp
            except Exception as e:
                if i < self.max_retries - 1:
                    time.sleep(1.5 ** i)
                else:
                    raise e

    def process(self, record):
        tax_id = record.get("tax_id")
        if not tax_id:
            record["ena_error"] = "Missing tax_id"
            yield record
            return

        if self.sleep_seconds:
            time.sleep(self.sleep_seconds)

        url = f"https://www.ebi.ac.uk/ena/browser/api/xml/{tax_id}"

        try:
            resp = self.get_with_retries(url)
            root = self.etree.fromstring(resp.content)
        except Exception as e:
            record["ena_error"] = f"Retry failed: {str(e)}"
            yield record
            return

        try:
            record["scientificName"] = root.find("taxon").get("scientificName")
        except Exception:
            record["ena_error"] = "Missing scientificName"
            yield record
            return

        if self.include_lineage:
            lineage_fields = ["kingdom", "phylum", "class", "order", "family", "genus"]
            for f in lineage_fields:
                record[f] = None
            try:
                for taxon in root.find("taxon").find("lineage").findall("taxon"):
                    rank = taxon.get("rank")
                    if rank in lineage_fields:
                        record[rank] = taxon.get("scientificName")
            except Exception:
                record["ena_warning"] = "Lineage parsing failed"

        yield record


class ValidateNamesFn(DoFn):
    VALIDATED = 'validated'
    TO_CHECK = 'to_check'

    def process(self, record):
        name = record.get('species')
        if not name:
            yield pvalue.TaggedOutput(self.TO_CHECK, record)
            return

        gb = gbif_spp.name_backbone(name=name, rank='species', strict=False, verbose=False)
        record.update({
            'gbif_matchType': gb.get('matchType'),
            'gbif_confidence': gb.get('confidence'),
            'gbif_scientificName': gb.get('scientificName'),
            'gbif_usageKey': gb.get('usageKey'),
            'gbif_status': gb.get('status'),
            'gbif_rank': gb.get('rank')
        })

        mt = gb.get('matchType')
        st = gb.get('status')
        if mt == 'NONE' or mt != 'EXACT' or st == 'SYNONYM':
            if gb.get('acceptedUsageKey'):
                record['gbif_acceptedUsageKey'] = gb.get('acceptedUsageKey')
            if gb.get('alternatives'):
                record['gbif_alternatives'] = gb.get('alternatives')
            yield pvalue.TaggedOutput(self.TO_CHECK, record)
        else:
            yield record


class WriteSpeciesOccurrencesFn(DoFn):
    """
    For each validated species record, fetches GBIF occurrences and writes to one file per species.
    Tracks success, skipped, and failed records using Beam metrics.
    """

    def __init__(self, output_dir, max_records=150):
        self.output_dir = output_dir
        self.max_records = max_records

        # Beam metrics
        self.success_counter = Metrics.counter("WriteSpeciesOccurrencesFn", "SUCCESS")
        self.skipped_counter = Metrics.counter("WriteSpeciesOccurrencesFn", "SKIPPED")
        self.failure_counter = Metrics.counter("WriteSpeciesOccurrencesFn", "FAILURES")

    def setup(self):
        self.gbif_client = gbif_occ

    def process(self, record):
        species = record.get('species')
        usage_key = record.get('gbif_usageKey')

        if not species or usage_key is None:
            self.skipped_counter.inc()
            yield {'species': species, 'status': 'skipped'}
            return

        safe_name = sanitize_species_name(species)
        filename = f"occ_{safe_name}.jsonl"
        out_path = f"{self.output_dir}/{filename}"
        tmp_path = out_path + ".tmp"

        try:
            resp = self.gbif_client.search(
                taxonKey=usage_key,
                basisOfRecord=['PRESERVED_SPECIMEN', 'MATERIAL_SAMPLE'],
                occurrenceStatus='PRESENT',
                hasCoordinate=True,
                hasGeospatialIssue=False,
                limit=self.max_records
            )

            occurrences = resp.get('results', [])
            lines = []

            for occ in occurrences:
                occ_out = {
                    'accession': record.get('accession'),
                    'tax_id': record.get('tax_id'),
                    'gbif_usageKey': occ.get('taxonKey'),
                    'species': occ.get('species'),
                    'decimalLatitude': occ.get('decimalLatitude'),
                    'decimalLongitude': occ.get('decimalLongitude'),
                    'coordinateUncertaintyInMeters': occ.get('coordinateUncertaintyInMeters'),
                    'eventDate': occ.get('eventDate'),
                    'countryCode': occ.get('countryCode'),
                    'basisOfRecord': occ.get('basisOfRecord'),
                    'occurrenceID': occ.get('occurrenceID'),
                    'gbifID': occ.get('gbifID')
                }
                lines.append(json.dumps(occ_out))

            with FileSystems.create(tmp_path) as f:
                for line in lines:
                    f.write((line + '\n').encode('utf-8'))
            FileSystems.rename([tmp_path], [out_path])

            self.success_counter.inc()
            yield {'species': species, 'count': len(occurrences)}

        except Exception as e:
            self.failure_counter.inc()
            yield pvalue.TaggedOutput('dead', {
                'species': species,
                'error': str(e)
            })
