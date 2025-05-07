import json
import requests
from lxml import etree
from elasticsearch import Elasticsearch
from apache_beam import DoFn, pvalue, metrics, io
from apache_beam.io.filesystems import FileSystems
from pygbif import species as gbif_spp, occurrences as gbif_occ
from utils.helpers import sanitize_species_name


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


# class ENATaxonomyFn(DoFn):
#     def __init__(self, include_lineage=False):
#         self.include_lineage = include_lineage
#
#     def process(self, record):
#         tax_id = record.get('tax_id')
#         if not tax_id:
#             yield record
#             return
#
#         url = f'https://www.ebi.ac.uk/ena/browser/api/xml/{tax_id}'
#         resp = requests.get(url)
#         root = etree.fromstring(resp.content)
#
#         record['scientificName'] = root.find('taxon').get('scientificName')
#         if self.include_lineage:
#             ranks = ['kingdom', 'phylum', 'class', 'order', 'family', 'genus']
#             for rank in ranks:
#                 record[rank] = None
#             lineage = root.find('taxon/lineage')
#             if lineage is not None:
#                 for tx in lineage.findall('taxon'):
#                     r = tx.get('rank')
#                     if r in ranks:
#                         record[r] = tx.get('scientificName')
#         yield record

class ENATaxonomyFn(DoFn):
    """
    Fetches taxonomy information from ENA using the species tax_id.
    Yields enriched records or records with an 'ena_error' field if the request or parsing fails.
    """

    def __init__(self, include_lineage: bool = True):
        self.include_lineage = include_lineage
        self.timeout = 10  # seconds

    def process(self, record):
        tax_id = record.get('tax_id')
        if not tax_id:
            record['ena_error'] = 'Missing tax_id'
            yield record
            return

        url = f'https://www.ebi.ac.uk/ena/browser/api/xml/{tax_id}'

        try:
            resp = requests.get(url, timeout=self.timeout)
            resp.raise_for_status()
            root = etree.fromstring(resp.content)
        except (requests.RequestException, etree.XMLSyntaxError) as e:
            record['ena_error'] = str(e)
            yield record
            return

        try:
            record['scientificName'] = root.find('taxon').get('scientificName')
        except Exception:
            record['ena_error'] = 'Missing scientificName'
            yield record
            return

        if self.include_lineage:
            lineage_fields = ['kingdom', 'phylum', 'class', 'order', 'family', 'genus']
            for rank in lineage_fields:
                record[rank] = None
            try:
                for taxon in root.find('taxon').find('lineage').findall('taxon'):
                    rank = taxon.get('rank')
                    if rank in lineage_fields:
                        record[rank] = taxon.get('scientificName')
            except Exception:
                record['ena_warning'] = 'Lineage parsing failed'

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
    SUCCESS = metrics.Metrics.counter('WriteOcc', 'success')
    SKIPPED = metrics.Metrics.counter('WriteOcc', 'skipped')
    FAILURES = metrics.Metrics.counter('WriteOcc', 'failures')

    def __init__(self, output_dir, max_records=150):
        self.output_dir = output_dir
        self.max_records = max_records

    def setup(self):
        self.gbif_client = gbif_occ

    def process(self, record):
        species = record.get('species')
        key = record.get('gbif_usageKey')
        if not species or key is None:
            return

        safe_name = sanitize_species_name(species)
        filename = f"occ_{safe_name}.jsonl"
        out_path = f"{self.output_dir}/{filename}"
        tmp_path = out_path + '.tmp'

        if FileSystems.exists(out_path):
            self.SKIPPED.inc()
            yield {'species': species, 'status': 'skipped'}
            return

        try:
            resp = self.gbif_client.search(
                taxonKey=key,
                basisOfRecord=['PRESERVED_SPECIMEN', 'MATERIAL_SAMPLE'],
                occurrenceStatus='PRESENT',
                hasCoordinate=True,
                hasGeospatialIssue=False,
                limit=self.max_records
            )
            results = resp.get('results', [])

            lines = [json.dumps({
                'accession': record.get('accession'),
                'tax_id': record.get('tax_id'),
                'gbif_usageKey': o.get('taxonKey'),
                'species': o.get('species'),
                'decimalLatitude': o.get('decimalLatitude'),
                'decimalLongitude': o.get('decimalLongitude'),
                'coordinateUncertaintyInMeters': o.get('coordinateUncertaintyInMeters'),
                'eventDate': o.get('eventDate'),
                'countryCode': o.get('countryCode'),
                'basisOfRecord': o.get('basisOfRecord'),
                'occurrenceID': o.get('occurrenceID'),
                'gbifID': o.get('gbifID')
            }) for o in results]

            with FileSystems.create(tmp_path) as fh:
                for line in lines:
                    fh.write((line + '\n').encode('utf-8'))

            FileSystems.rename([tmp_path], [out_path])
            self.SUCCESS.inc()
            yield {'species': species, 'count': len(results)}

        except Exception as e:
            self.FAILURES.inc()
            yield pvalue.TaggedOutput('dead', {
                'species': species,
                'error': str(e)
            })
