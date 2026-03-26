# Biodiversity pipelines

Apache Beam batch pipelines for taxonomy validation, occurrence retrieval, cleaning, spatial annotation, range estimation, and provenance export.

## Quick start

### 1. Install dependencies

Use Python 3.12 and install the project dependencies:

```bash
pip install -r requirements.txt
```

### 2. Prerequisites

* Python 3.11
* Access to Elasticsearch (host, user, password, index)
* Local disk space for intermediate outputs (`out/`)

### 3. Required data

Download and place the following datasets locally (or use accessible paths):

* Land polygons (Natural Earth):

```bash
# example structure
data/spatial_processing/ne_10m_land/ne_10m_land.shp
```

* Administrative centroids:

```bash
data/spatial_processing/ne_10m_admin_0_label_points/ne_10m_admin_0_label_points.shp
```

* Climate rasters (CHELSA):

```bash
data/climate/*.tif
```

* Biogeographic regions (Ecoregions):

```bash
data/bioregions/Ecoregions2017.shp
```

### 4. Project layout

Main pipeline entry points:

* `taxonomy_pipeline.py`
* `occurrences_pipeline.py`
* `cleaning_occs_pipeline.py`
* `spatial_annotation_pipeline.py`
* `range_estimation_pipeline.py`
* `data_provenance_pipeline.py`

### 3. Run pattern

Each pipeline follows the same local pattern:

```bash
python -m biodiv_pipelines.<pipeline> --arg1 value1 --arg2 value2
```

Beam runner options can also be passed after the pipeline arguments when needed.

## Pipeline order

Recommended execution order:

1. Taxonomy
2. Occurrences
3. Cleaning
4. Spatial annotation
5. Range estimation
6. Data provenance

## Minimal local execution (development)

Run pipelines locally using the DirectRunner (default).

Ensure all input data exists in GCS or in your local directories before running (schemas, climate, bioregions, etc.).

Define your variables:

```bash
export PROJECT_ID=<project-id>
export REGION=<your-region>
export BUCKET=gs://<your-bucket>
export BQ_DATASET=<your-bq-dataset>
```
### Taxonomy

```bash
python -m biodiv_pipelines.taxonomy_pipeline \
  --host <host> \
  --user <user> \
  --password <password> \
  --index <index> \
  --output "${BUCKET}/out/taxonomy/taxonomy" \
  --size 10 \
  --pages 1 \
  --sleep 0.25 \
  --bq_table "${PROJECT_ID}.${BQ_DATASET}.bq_taxonomy_validated" \
  --bq_schema "${BUCKET}/schemas/bq_taxonomy_schema.json" \
  --bq_gate_table "${PROJECT_ID}.${BQ_DATASET}.bp_log_taxonomy" \
  --temp_location "${BUCKET}/temp" \
  --project "${PROJECT_ID}" \
  --runner DirectRunner
```

**Supports:**
- Pagination control via `--size` and `--pages`
- Fetch-all mode (`--pages 0`)
- Incremental processing of new species only when `--bq_gate_table` is provided
- BigQuery output only when `--bq_table`. `--bq_schema` and `--temp_location` are provided.
- `--output` can be a local file or a GCS path

**Expected output:**
```text
out/taxonomy/
├── taxonomy_validated.jsonl
└── taxonomy_tocheck.jsonl
```
- `taxonomy_validated.jsonl`: species validated against GBIF and ready for the occurrences pipeline
- `taxonomy_tocheck.jsonl`: unresolved or lower-confidence matches that need manual review. Contains species with FUZZY, HIGHER RANK, and SYNONYMS matches.


### Occurrences

```bash
python -m biodiv_pipelines.occurrences_pipeline \
  --validated_input "${BUCKET}/out/taxonomy/taxonomy_validated.jsonl" \
  --output_dir "${BUCKET}/out/occurrences/raw" \
  --limit 10 \
  --runner DirectRunner
```
**NOTE**: using pygbif module occurrences.search.

**Supports:**
- `--limit` to limit the number of occurrences downloaded. Three hundred max.
- `--output_dir` to specify the output directory for raw occurrence records
- `--validated_input` and `--output_dir` support local files and GCS paths.

**Expected output:**
```text
out/occurrences/
├── occ_<species_name>.jsonl
├── summary_occ_download.jsonl
└── dead_records.jsonl   # only if failures occur
```

- `occ_<species_name>.jsonl`: one file per validated species with GBIF occurrence records
- `summary_occ_download.jsonl`: aggregate counts for succeeded species, failed species, and written occurrences
- `dead_records.jsonl`: failed GBIF retrievals, if any


### Cleaning

```bash
python -m biodiv_pipelines.cleaning_occs_pipeline \
  --input_glob "${BUCKET}/out/occurrences/occ_*.jsonl" \
  --output_dir "${BUCKET}/out/occurrences/clean" \
  --land_shapefile "${BUCKET}/data/spatial_processing/ne_10m_land/ne_10m_land.shp" \
  --centroid_shapefile "${BUCKET}/data/spatial_processing/ne_10m_admin_0_label_points/ne_10m_admin_0_label_points.shp" \
  --min_uncertainty 1000 \
  --max_uncertainty 5000 \
  --max_centroid_dist 5000 \
  --output_consolidated "${BUCKET}/out/pp/occurrences/clean/all_species" \
  --bq_table "${PROJECT_ID}.${BQ_DATASET}.bp_gbif_occurrences" \
  --bq_schema "${BUCKET}/schemas/bq_gbif_occurrences_schema.json" \
  --temp_location "${BUCKET}/temp" \
  --staging_location "${BUCKET}/staging" \
  --project "${PROJECT_ID}" \
  --runner DirectRunner
```
Filter raw occurrence records by removing invalid coordinates, centroids, marine points, and duplicates.

**Supports:**
- `land_shapefile` and `centroid_shapefile` are paths to shapefiles containing the land and centroids of the world.
- `min_uncertainty` and `max_uncertainty` are the maximum allowed uncertainty in meters for coordinates.
- `max_centroid_dist` is the maximum allowed distance in meters between a point and its centroid.
- `--output_consolidated` can be a local file or a GCS path.
- BigQuery output only when `--bq_table`, `--bq_schema`, and `--temp_location` are provided.
- All input and output files can be local or GCS paths.

**Expected output:**
```text
```text
out/occurrences/clean/
├── occ_<species_name>.jsonl
└── all_species.jsonl    # only if --output_consolidated is provided
```

- `occ_<species_name>.jsonl`: cleaned and deduplicated occurrences per species
- `all_species.jsonl`: consolidated cleaned occurrences across all species if `--output_consolidated` is provided.


### Spatial annotation

```bash
python -m biodiv_pipelines.spatial_annotation_pipeline \
  --input_occs "${BUCKET}/out/occurrences_clean/occ_*.jsonl" \
  --climate_dir "${BUCKET}/data/climate" \
  --biogeo_vector "${BUCKET}/data/bioregions/Ecoregions2017.shp" \
  --annotated_output "${BUCKET}/out/spatial/annotated" \
  --summary_output "${BUCKET}/out/spatial/summary" \
  --bq_summary_table "${PROJECT_ID}.${BQ_DATASET}.bp_spatial_annotations" \
  --bq_schema "${BUCKET}/schemas/bq_spatial_annotation_summ_schema.json" \
  --temp_location "${BUCKET}/temp" \
  --staging_location "${BUCKET}/staging" \
  --project "${PROJECT_ID}" \
  --runner DirectRunner
```
Use cleaned occurrence records to extract climate and area classification information from spatial data layers. At the moment: CHELSA climatologies and WWF Ecorregions (Dinnerstein et al. 2017).

**Supports:**
- `climate_dir` and `biogeo_vector` are paths to shapefiles containing the land and centroids of the world.
- `annotated_output` and `summary_output` are paths to output directories.
- BigQuery output only when `--bq_summary_table`, `--bq_schema`, and `--temp_location` are provided.
- All input and output files can be local or GCS paths.

**Expected outpur:**
```text
out/spatial/
├── annotated.jsonl
└── summary.jsonl
```

- `annotated.jsonl`: occurrence-level climate and biogeographic annotations
- `summary.jsonl`: accession-level summary statistics for climate and biogeographic regions


### Range estimation

This pipeline uses the `range_km2` field in the occurrence records to estimate the geographic range of each species.

It does not produce a text file output like the other pipelines. Instead, it writes the results to a BigQuery table.

```bash
python range_estimation_pipeline.py \
  --input_glob "${BUCKET}/out/occurrences/clean/occ_*.jsonl" \
  --bq_table "${PROJECT_ID}.${BQ_DATASET}.bp_species_range_estimates" \
  --bq_schema "${BUCKET}/schemas/bq_range_estimates_schema.json" \
  --temp_location "${BUCKET}/temp" \
  --staging_location "${BUCKET}/staging" \
  --project "${PROJECT_ID}" \
  --runner DirectRunner
```
**Expected output:**
- One row per species with estimated convex hull range area in km²
- If fewer than 3 valid coordinates are available, range_km2 is null and a note is added


### Data provenance

```bash
python -m biodiv_pipelines.data_provenance_pipeline \
  --taxonomy_path "${BUCKET}/out/taxonomy_validated.jsonl" \
  --host <host> \
  --user <user> \
  --password <password> \
  --index <index> \
  --min_batch_size 50 \
  --max_batch_size 200 \
  --taxonomy_path "${BUCKET}/out/taxonomy/taxonomy_validated.jsonl" \
  --output "${BUCKET}/out/metadata/provenance_urls_new.jsonl" \
  --bq_table "${PROJECT_ID}.${BQ_DATASET}.bp_provenance_metadata" \
  --bq_schema "${BUCKET}/schemas/bq_metadata_url_schema.json" \
  --temp_location "${BUCKET}/temp" \
  --staging_location "${BUCKET}/staging" \
  --project "${PROJECT_ID}" \
  --runner DirectRunner
```
**Supports:**
- `min_batch_size` and `max_batch_size` control the number of species per batch in Beam. Adjust only if there are performance issues.
- BigQuery output only when `--bq_table`, `--bq_schema`, and `--temp_location` are provided.
- All input and output files can be local or GCS paths.

**Expected output:**
```text
out/metadata/
└── provenance_urls_new.jsonl
```
- One row per species with source URLs and identifiers, including Biodiversity Portal, GTF, Ensembl browser, and GBIF links.


## Run on Google Cloud Dataflow

Use Dataflow after validating the pipelines locally.

### 1. Build and push the Docker image

Define variables:

```bash
export PROJECT_ID=<project-id>
export REGION=<your-region>
export BUCKET=gs://<your-bucket>
export TAG=<your-tag>
export IMAGE=${REGION}-docker.pkg.dev/${PROJECT_ID}/biodiversity-images/biodiv-pipeline:${TAG}
```

### Build and push the Docker image

In the project root, execute the following commands:

Using Docker:

```bash
docker build --no-cache -f Dockerfile -t ${IMAGE} .
docker push ${IMAGE}
```
Or directly on Google Cloud using the gcloud library:

```bash
gcloud builds submit . \
  --tag ${IMAGE} \
  --project ${PROJECT_ID}
```

### 2. Example: Taxonomy pipeline on Dataflow

```bash
python -m biodiv_pipelines.taxonomy_pipeline \
  --host <host> \
  --user <user> \
  --password <password> \
  --index <index> \
  --size 10 \
  --pages 1 \
  --sleep 0.25 \
  --output "${BUCKET}/out/taxonomy/taxonomy" \
  --bq_table "${PROJECT_ID}.${BQ_DATASET}.bq_taxonomy_validated" \
  --bq_schema "${BUCKET}/schemas/bq_taxonomy_schema.json" \
  --bq_gate_table "${PROJECT_ID}.${BQ_DATASET}.bp_log_taxonomy" \
  --runner DataflowRunner \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --temp_location "${BUCKET}/temp" \
  --staging_location "${BUCKET}/staging" \
  --sdk_container_image "${IMAGE}" \
  --save_main_session
```

### 3. Pattern for other pipelines

To run any other pipeline on Dataflow:

1. Start from the local command
2. Replace:
local paths → `gs://` paths & `--runner DirectRunner `→ `--runner DataflowRunner`
3. Ensure the following flags are always present:

```bash
--runner DataflowRunner \
--project "${PROJECT_ID}" \
--region "${REGION}" \
--temp_location "${BUCKET}/temp" \
--staging_location "${BUCKET}/staging" \
--sdk_container_image "${IMAGE}" \
--save_main_session
```

## Run using Dataflow Flex Templates

For production and orchestration (e.g. Cloud Composer), pipelines should be executed using Dataflow Flex Templates instead of direct `python -m` submission.

Flex Templates package:
- the pipeline code
- dependencies (via Docker image)
- runtime parameters

---

### 1. Build and register the Flex Template

Define variables:

```bash
export PROJECT_ID=<project-id>
export REGION=<region>
export BUCKET=gs://<your-bucket>
export TAG=<your-tag>
export IMAGE=${REGION}-docker.pkg.dev/${PROJECT_ID}/biodiversity-images/biodiversity-flex:${TAG}
export TEMPLATE_PATH=${BUCKET}/flex-templates/flex-<pipeline>.json
```

### 2. Build and push the container image

```bash
gcloud builds submit . \
  --tag ${IMAGE} \
  --project ${PROJECT_ID}
```

### 3. Build the Flex Template

```bash
gcloud dataflow flex-template build ${TEMPLATE_PATH} \
  --image=${IMAGE} \
  --sdk-language=PYTHON \
  --metadata-file=metadata_<pipeline>.json \
  --project=${PROJECT_ID}
```

### 4. Run the Flex Template

```bash
gcloud dataflow flex-template run taxonomy-job-$(date +%Y%m%d-%H%M%S) \
  --template-file-gcs-location=${TEMPLATE_PATH} \
  --region=${REGION} \
  --parameters pipeline=taxonomy \
  --parameters host=<host> \
  --parameters user=<user> \
  --parameters password=<password> \
  --parameters index=<index> \
  --parameters size=10 \
  --parameters pages=1 \
  --parameters sleep=0.25 \
  --parameters output="${BUCKET}/out/taxonomy/taxonomy" \
  --parameters bq_table="${PROJECT_ID}.${BQ_DATASET}.bq_taxonomy_validated" \
  --parameters bq_schema="${BUCKET}/schemas/bq_taxonomy_schema.json" \
  --parameters bq_gate_table="${PROJECT_ID}.${BQ_DATASET}.bp_log_taxonomy" \
  --parameters temp_location="${BUCKET}/temp" \
  --parameters sdk_container_image=${IMAGE} \
  --parameters experiments=use_runner_v2
```


---
OLD - TO DELETE LATER
---
---
# Biodiversity Pipeline (Apache Beam)

This project defines a modular data pipeline built with Apache Beam. The workflow consists of four primary pipelines that process biological species data from retrieval through spatial annotation and summarisation.

---

## Project Structure

```bash

.
├── README.md
├── data
│   ├── climate/                          # CHELSA raster climate data
│   ├── bioregions/                       # WWF Ecoregion shapefiles
│   └── spatial_processing/               # Natural Earth land and centroid shapefiles
├── out/
│   ├── validated_taxonomy/               # Taxonomy validation outputs
│   ├── occurrences_raw/                  # Raw GBIF occurrence records
│   ├── occurrences_clean/                # Cleaned GBIF occurrences
│   └── spatial/                          # Annotated occurrence records and summaries
├── docker                                
│   └── Dockerfile.dataflow               # Docker file for building the Dataflow Image.
├── pyproject.toml
├── requirements.txt
└── src
    └── biodiv_pipelines
        ├── __init__.py
        ├── taxonomy_pipeline.py              # Retrieves and validates taxonomy of species with complete genome annotations
        ├── occurrences_pipeline.py           # Search and downloads a sample of GBIF occurrences using usageKeys
        ├── cleaning_occs_pipeline.py         # Cleans and deduplicates occurrence records
        ├── spatial_annotation_pipeline.py    # Annotates coordinates with climate and biogeographic data
        ├── range_estimation_pipeline.py      # Estimates species' geographic range area (Extent of occurrence/Convex Hull)
        ├── data_provenance_pipeline.py       # Collects links from data sources per species
        ├── cleaning_summary_pipeline.py 
        ├── climate_summary_pipeline.py
        ├── biogeo_summary_pipeline.py
        └── utils
            ├── __init__.py
            ├── bq_gbif_occurrences_schema.json
            ├── bq_metadata_url_schema.json
            ├── bq_range_estimates_schema.json
            ├── bq_spatial_annotation_schema.json
            ├── bq_spatial_annotation_summ_schema.json
            ├── bq_taxonomy_schema.json
            ├── cleaning_occs.py               # Cleaning logic for occurrence data.
            ├── helpers.py                     # Shared utilities (schema conversion, I/O helpers, etc.)
            └── transforms.py                  # Apache Beam DoFns for data processing

```


## Workflow Overview

1. **Taxonomy pipeline**  
Fetch species annotations from Elasticsearch, optionally filter out already processed species using a BigQuery gate table, enrich them with ENA taxonomy data, and validate scientific names via GBIF.

Supports:
- Pagination control via `--size` and `--pages`
- Fetch-all mode (`--pages 0`)
- Incremental processing of new species only when `--bq_gate_table` is provided

2. **Occurrences pipeline**  
   Download GBIF occurrence records using validated GBIF usageKeys.

3. **Cleaning pipeline**  
   Filter raw occurrence records by removing invalid coordinates, centroids, marine points, and duplicates.

4. **Spatial annotation pipeline**  
   Generate spatial buffers around cleaned occurrences and annotate them with:
   - Climate data (CHELSA rasters)
   - Biogeographic regions (WWF ecoregions)
   - Produces:
     - Per-occurrence annotations
     - Per-accession summary statistics

5. **Range estimation pipeline**  
   Calculate the geographic range (in km²) for each species by computing the convex hull of its occurrence coordinates.

---

## Local Execution

### Install the biodiv_pipelines package

```bash
pip install -e .
```

### 1. Taxonomy pipeline
Fetch genome metadata from Elasticsearch, enrich with ENA taxonomy, and validate species names using GBIF.

Options:

- `--pages N` limits the number of Elasticsearch pages retrieved.
- `--pages 0` fetches all available species (until exhaustion).
- If `--bq_gate_table` is provided, the pipeline processes only species whose `tax_id` is not already present in the specified BigQuery table.

```bash
python -m biodiv_pipelines.taxonomy_pipeline \
  --host localhost \
  --user elastic \
  --password yourpassword \
  --index your_es_index \
  --output "out/validated_taxonomy/taxonomy" \
  --size 10 \
  --pages 1 \
  --sleep 0.25 \
  --direct_num_workers=1
```
Outputs:

- `taxonomy_validated.jsonl`
- `taxonomy_tocheck.jsonl` (**NOTE**: Contains species with FUZZY, HIGHER RANK, and SYNONYMS matches. Logic to deal with these species is still in development.) 

### 2. Occurrences pipeline

Download GBIF occurrences for validated species using their GBIF usageKey. (**NOTE**: using occurrences.search for now.)

```bash
python -m biodiv_pipelines.occurrences_pipeline \
  --validated_input out/validated_taxonomy/taxonomy_validated.jsonl \
  --output_dir out/occurrences_raw \
  --limit 100
  --direct_num_workers=1
```
Produces:

* One `occ_<sp_name>.jsonl` file per species in data/occurrences_raw/
* `summary_occ_download.jsonl` with Beam metrics: `{"SUCCESS": 124, "SKIPPED": 3, "FAILURES": 1}` 
* `dead_records.jsonl` (if any failures, e.g., GBIF API failures)

### 3. Cleaning pipeline

Filter and deduplicate raw occurrence records. Removes invalid coordinates, zero-coordinates, coordinates at the sea, and administrative centroids.

```bash
python -m biodiv_pipelines.cleaning_occs_pipeline \
  --input_glob "out/occurrences_raw/*.jsonl" \
  --output_dir "out/occurrences_clean" \
  --land_shapefile "data/spatial_processing/ne_10m_land.zip" \
  --centroid_shapefile "data/spatial_processing/ne_10m_admin_0_label_points.zip" \
  --output_consolidated "out/occurrences_clean/all_species" \
  --max_uncertainty 1000 \
  --max_centroid_dist 5000 
  ```
Outputs:

- Cleaned per-species `occ_<sp_name>.jsonl` files in `out/occurrences_clean/`

### 4. Spatial annotation pipeline

Use cleaned occurrence records to extract climate and area classification information from spatial data layers. At the moment: CHELSA climatologies and WWF Ecorregions (Dinnerstein et al. 2017).

```bash
python -m biodiv_pipelines.spatial_annotation_pipeline \
  --input_occs "out/occurrences_clean/occ_*.jsonl" \
  --climate_dir "data/climate" \
  --biogeo_vector "data/bioregions/Ecoregions2017.zip" \
  --annotated_output "out/spatial/annotated" \
  --summary_output "out/spatial/summary" 
```
Outputs:

- Annotated occurrence records in `out/spatial/annotated,jsonl`
- Summary per accession in `out/spatial/summary.jsonl`

### 5. Range estimation pipeline

```bash
python -m biodiv_pipelines.range_estimation_pipeline \
  --input_glob "out/occurrences_clean/occ_*.jsonl" \
  --bq_table your-project:your_dataset.range_estimates \
  --bq_schema "utils/bq_range_estimates_schema.json" \
  --temp_location gs://your-bucket/temp \
  --direct_num_workers=1
```
Outputs:

- A `.jsonl` file with the species and its estimated range size (i.e., area of convex hull in km<sup>2</sup>).

6. **Data provenance pipeline**  
Uses the validated taxonomy output to drive targeted Elasticsearch lookups (by `tax_id`) and retrieves provenance links per species (Biodiversity Portal, GTF, Ensembl browser). Adds a GBIF species URL and appends results to the existing BigQuery table `bp_provenance_metadata`.

### 6. Data provenance pipeline

Fetch provenance metadata for validated species only (incremental), by querying Elasticsearch using the `tax_id`s present in `taxonomy_validated.jsonl`.  
The pipeline batches requests to avoid excessive Elasticsearch calls and appends results to the existing BigQuery table `bp_provenance_metadata`.

Required input:
- `taxonomy_validated.jsonl` produced by the taxonomy pipeline

Output fields:
- `tax_id`, `accession`, `Biodiversity_portal`, `GTF`, `Ensembl_browser`, `gbif_url`

Local run (JSONL only):

```bash
python -m biodiv_pipelines.data_provenance2_pipeline \
  --taxonomy_path out/validated_taxonomy/taxonomy_validated.jsonl \
  --host localhost \
  --user elastic \
  --password yourpassword \
  --index your_es_index \
  --min_batch_size 50 \
  --max_batch_size 200 \
  --output "out/provenance/provenance" \
  --write_jsonl \
  --direct_num_workers=1
```
___
## Cloud Deployment (Google Cloud Dataflow)

Each pipeline can be deployed to Google Cloud Dataflow by:

- Replacing local paths with `gs://` paths
- Setting the `--runner` to `DataflowRunner`
- Including required GCP parameters:
  - `--project`, `--region`
  - `--temp_location`, `--staging_location`
  - `--requirements_file`
  - `--save_main_session` (for pipelines with imported modules)

### Docker Setup

To run the pipeline on Dataflow with custom dependencies or pinned versions, you need to build and push a custom Docker image using the provided Dockerfile.

#### Build and push the Docker image

1. Navigate to the root directory of the project:
```bash
   cd docker
```

2. Build the image using the provided `Dockerfile.dataflow`:

```bash
docker build --no-cache -f docker/Dockerfile.dataflow -t <region>-docker.pkg.dev/<project-id>/<repo>/biodiversity-pipeline:latest ..
```
3. Push the image to Google Artifact Registry:

```bash
docker push <region>-docker.pkg.dev/<project-id>/<repo>/biodiversity-pipeline:latest
```

### Example: Taxonomy pipeline

```bash
python -m biodiv_pipelines.taxonomy_pipeline \
  --host <secret> \
  --user <secret> \
  --password <secret> \
  --index <secret> \
  --size 10 \
  --pages 1 \
  --output gs://my-bucket/validated_taxonomy/taxonomy \
  --bq_table my-project:my_dataset.taxonomy \
  --bq_schema gs://my-bucket/utils/bq_taxonomy_schema.json \
  --bq_gate_table my-project:my_dataset.bq_taxonomy_gate \
  --runner DataflowRunner \
  --project my-project \
  --region my-region \
  --temp_location gs://my-bucket/temp \
  --staging_location gs://my-bucket/staging \
  --sdk_container_image <my-docker-image-in-GCS-artifact/biodiversity-pipeline:latest> \
  --save_main_session \
  --max_num_workers=2
```

When `--bq_gate_table` is specified, the pipeline reads existing `tax_id`s from this table and processes only new species. Both validated and to_check species are appended to the gate table with a timestamp and processing status.
### Example: Occurrence pipeline

```bash
python -m biodiv_pipelines.occurrences_pipeline \
  --validated_input gs://my-bucket/validated_taxonomy/test_taxonomy_validated.jsonl \
  --output_dir gs://my-bucket/test_jpng/out/occurrences_raw \
  --limit 300 \
  --runner DataflowRunner \
  --project my-project \
  --region my-region \
  --temp_location gs://my-bucket/temp \
  --staging_location gs://my-bucket/staging \
  --sdk_container_image <my-docker-image-in-GCS-artifact/biodiversity-pipeline:latest> \
  --save_main_session \
  --max_num_workers=2
```

### Example: Cleaning pipeline

```bash
python -m biodiv_pipelines.cleaning_occs_pipeline \
  --input_glob gs://my-bucket/out/occurrences_raw/occ_*.jsonl \
  --output_dir gs://my-bucket//out/occurrences_clean \
  --land_shapefile gs://my-bucket/data/spatial_processing/ne_10m_land/ne_10m_land.shp \
  --centroid_shapefile gs://my-bucket/data/spatial_processing/ne_10m_admin_0_label_points/ne_10m_admin_0_label_points.shp \
  --max_uncertainty 1000 \
  --max_centroid_dist 5000 \
  --shards 5 \
  --runner DataflowRunner \
  --project my-projects \
  --region my-region \
  --temp_location gs://my-bucket/temp \
  --staging_location gs://my-bucket/staging \
  --bq_table my-project:my_dataset.gbif_occurrences \
  --bq_schema gs://my-bucket/utils/bq_gbif_occurrences_schema.json \
  --sdk_container_image <my-docker-image-in-GCS-artifact/biodiversity-pipeline:latest> \
  --save_main_session \
  --max_num_workers=2
```

### Example: Spatial annotation pipeline

```bash
 python -m biodiv_pipelines.spatial_annotation_pipeline \
  --input_occs gs://my-bucket/out/occurrences_clean/*.jsonl \
  --climate_dir gs://my-bucket/data/climate \
  --biogeo_vector gs://my-bucket/data/bioregions/Ecoregions2017/Ecoregions2017.shp \
  --annotated_output gs://my-bucket/out/spatial/spatial_annotations \
  --summary_output gs://my-bucket/out/spatial/spatial_annotations_summary \
  --bq_summary_table my-project:my_dataset.spatial_annotations \
  --bq_schema gs://my-bucket/utils/bq_spatial_annotation_summ_schema.json \
  --runner DataflowRunner \
  --project my-project \
  --region my-region \
  --temp_location gs://my-bucket/temp \
  --staging_location gs://my-bucket/staging \
  --sdk_container_image <my-docker-image-in-GCS-artifact/biodiversity-pipeline:latest> \
  --save_main_session \
  --max_num_workers=2
```

### Example: Range estimation

```bash
python -m biodiv_pipelines.range_estimation_pipeline \
  --input_glob gs://my-bucket/out/occurrences_clean/*.jsonl \
  --bq_table my-project:my_dataset.species_range_estimates \
  --bq_schema gs://my-bucket/utils/bq_range_estimates_schema.json \
  --temp_location gs://my-bucket/temp \
  --runner DataflowRunner \
  --project my-project \
  --region my-region \
  --staging_location gs://my-bucket/staging \
  --sdk_container_image <my-docker-image-in-GCS-artifact/biodiversity-pipeline:latest> \
  --save_main_session \
  --max_num_workers=2
```

### Example: Data provenance

```bash
python -m biodiv_pipelines.data_provenance2_pipeline \
  --taxonomy_path out/validated_taxonomy/taxonomy_validated.jsonl \
  --host localhost \
  --user elastic \
  --password yourpassword \
  --index your_es_index \
  --min_batch_size 50 \
  --max_batch_size 200 \
  --bq_table my-project:my_dataset.bp_provenance_metadata \
  --bq_schema "gs://my-bucket/utils/bq_metadata_url_schema.json" \
  --temp_location gs://my-bucket/temp \
  --output "gs://my-bucket/out/provenance/provenance" \
  --runner DataflowRunner \
  --project my-project \
  --region my-region \
  --staging_location gs://my-bucket/staging \
  --save_main_session
  ```
**Notes:**

The pipeline is incremental because it only processes species present in taxonomy_validated.jsonl (which can itself be gated by the taxonomy pipeline gate table).

BigQuery writes use WRITE_APPEND into the existing bp_provenance_metadata table.