import json
import os
import re
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp.internal.clients import bigquery

def sanitize_species_name(species: str) -> str:
    """
    Extracts the genus and species epithet (first two words) from a species name
    and returns a sanitized string that can safely be used in file paths.
    """
    parts = species.strip().split()
    if not parts:
        return ''
    genus_species = '_'.join(parts[:2])
    safe = re.sub(r'[^A-Za-z0-9_]', '_', genus_species)
    safe = re.sub(r'_+', '_', safe).strip('_')
    return safe


def extract_species_name(file_path: str) -> str:
    """
    Extracts the species name from a file path like 'occ_Panthera_leo.jsonl'
    and converts it to a space-separated name like 'Panthera leo'.
    """
    match = re.search(r'occ_(.+?)\.jsonl$', file_path)
    return match.group(1).replace('_', ' ') if match else "Unknown species"

def write_species_file(kv, output_dir):
    """
    Writes JSONL records for a single species to a file in the output directory.
    `kv`: tuple (species_name, iterable of json strings)
    """
    (species_name, _shard), records = kv
    safe_name = re.sub(r'[^A-Za-z0-9_]', '_', species_name.replace(' ', '_'))
    path = os.path.join(output_dir, f'occ_{safe_name}.jsonl')
    with FileSystems.create(path) as f:
        for line in records:
            f.write((json.dumps(line) + "\n").encode("utf-8"))


def merge_annotations(inputs):
    """
    Merges climate and biogeo annorations keyed by occurrenceID into a single pcollection.
    :param inputs: mapped pcollection using occurrenceID
    :return:
    """
    merged = {}
    for src in ["climate", "biogeo"]:
        recs = inputs.get(src, [])
        if recs:
            merged.update(recs[0])
    return merged


def convert_dict_to_table_schema(schema_dict_list):
    """
    Converts a list of schema dicts (from JSON) into a Beam-compatible TableSchema.
    """
    table_schema = bigquery.TableSchema()
    for field in schema_dict_list:
        table_field = bigquery.TableFieldSchema()
        table_field.name = field["name"]
        table_field.type = field["type"]
        table_field.mode = field.get("mode", "NULLABLE")
        table_schema.fields.append(table_field)
    return table_schema